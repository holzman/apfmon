from apfmon.mon.models import Factory
from apfmon.mon.models import Job
from apfmon.mon.models import Label

from apfmon.kit.models import Site
from apfmon.kit.models import BatchQueue
from apfmon.kit.models import WMSQueue
from apfmon.kit.models import CLOUDS

import csv
import logging
import math
import pytz
import re
import redis
import statsd
import string
import sys
import time
from operator import itemgetter
from datetime import timedelta, datetime
from django.shortcuts import redirect, render_to_response, get_object_or_404
from django.db.models import Count
from django.db.models import Q
from django.http import HttpResponse, Http404, HttpResponseBadRequest
from django.http import HttpResponseRedirect
from django.core.cache import cache
from django.views.decorators.cache import cache_page
from django.conf import settings
from django.core.context_processors import csrf
from django.core.mail import mail_managers
from django.core.paginator import Paginator, InvalidPage, EmptyPage
from django.core.serializers.json import DjangoJSONEncoder
from django.core.urlresolvers import reverse
from django.core.exceptions import MultipleObjectsReturned

try:
    import json as json
except ImportError, err:
    logging.error('Cannot import json, using simplejson')
    import simplejson as json

ELOGREGEX = re.compile('(.*elog[^0-9]*)([0-9]+)', re.IGNORECASE)
SAVANNAHREGEX = re.compile('(.*savannah[^0-9]*)([0-9]+)', re.IGNORECASE)
GGUSREGEX = re.compile('(.*ggus[^0-9]*)([0-9]+)', re.IGNORECASE)
ELOGURL = 'https://atlas-logbook.cern.ch/elog/ATLAS+Computer+Operations+Logbook/%s'
GGUSURL = 'https://ggus.eu/ws/ticket_info.php?ticket=%s'
SAVANNAHURL = 'https://savannah.cern.ch/support/?%s'
#CLOUDS = [ 'CA', 'CERN', 'DE', 'ES', 'FR', 'IT', 'ND', 'NL', 'RU', 'TW', 'UK', 'US']
CLOUDLIST = []
for item in CLOUDS:
  CLOUDLIST.append(item[0])

ss = statsd.StatsClient(settings.GRAPHITE['host'], settings.GRAPHITE['port'])
red = redis.StrictRedis(settings.REDIS['host'] , port=settings.REDIS['port'], db=0)
expire2days = 172800
expire7days = 604800
expire3hrs = 3*3600
span = 7200
interval = 300

# Flows
# 1. CREATED <- condor_id (Entry)
# 3. RUNNING <- signal from pilot-wrapper
# 4. EXITING <- signal from pilot-wrapper
# 5. DONE <- signal from cronjob script (mon-expire.py) jobstate=4

def jobs1(request, lid, state, p=1):
    """
    Rendered view of a set of Jobs for particular Label and optional state
    """

    lab = get_object_or_404(Label, id=int(lid))

    jobs = Job.objects.filter(label=lab,state=state)
    jobs = jobs.order_by('-last_modified')
    pages = Paginator(jobs, 100)

    try:
        page = pages.page(p)
    except (EmptyPage, InvalidPage):
        page = pages.page(pages.num_pages)

    context = {
                'label' : lab,
                'name' : lab.name,
                'factoryname' : lab.fid.name,
                'fid' : lab.fid.id,
                'pandaq' : lab.batchqueue,
                'statename' : state,
                'jobs' : jobs,
                'pages' : pages,
                'page' : page,
                }

    return render_to_response('mon/jobs.html', context)


def job1(request, fid, cid):
    """
    Rendered view of job information
    """

    # handle both Factory id and name
    try:
        id = int(fid)
        f = get_object_or_404(Factory, id=id)
    except ValueError:
        f = get_object_or_404(Factory, name=fid)
        
    jid = ':'.join((f.name,cid))
    job = get_object_or_404(Job, jid=jid)

    msglist = red.lrange(job.jid, 0, -1)

    msgs = []
    for msg in msglist:
        # msg is a string with format:
        # "<epoch_time> <client_ip> <some message with spaces>"
        (t, ip, txt) = msg.split(' ',2)
        msg = {'received' : datetime.fromtimestamp(float(t)),
               'client'   : ip,
               'msg'      : txt, 
             }
 
        msgs.append(msg)

    date = ''
    if f.factory_type != 'glideinWMS':
        date = "%d-%02d-%02d" % (job.created.year, job.created.month, job.created.day)
    # these need to come from Factory info
    out = "%s/%s/%s/%s.out"
    err = "%s/%s/%s/%s.err"
    log = "%s/%s/%s/%s.log"

    dir = str(job.label).translate(string.maketrans('/:','__'))

    outurl = out % (f.url, date, dir, job.cid)
    errurl = err % (f.url, date, dir, job.cid)
    logurl = log % (f.url, date, dir, job.cid)

    
    # datetime.fromtimestamp(time.time())
    context = {
                'outurl'  : outurl,
                'errurl'  : errurl,
                'logurl'  : logurl,
                'factory' : f,
                'job'     : job,
                'msgs'    : msgs,
                }

    return render_to_response('mon/job.html', context)

@cache_page(60 * 10)
def factory(request, fid):
    """
    Rendered view of Factory instance. Lists all factory labels with
    a count of jobs in each state.
    """

    try:
        id = int(fid)
    except ValueError:
        raise Http404

    dtdead = datetime.now(pytz.utc) - timedelta(days=10)

    f = get_object_or_404(Factory, id=id)
    pandaqs = BatchQueue.objects.all()
    labels = Label.objects.filter(fid=f, last_modified__gt=dtdead)
    jobs = Job.objects.filter(label__fid=f)
    dt = datetime.now(pytz.utc) - timedelta(hours=1)
    dtlab = datetime.now(pytz.utc) - timedelta(weeks=3)

    lifetime = 300
    rows = []
    for lab in labels:
        if lab.last_modified < dtlab:
            # todo: mark label inactive lab.save()
            continue
        ncreated = 0
        nsubmitted = 0
        nrunning = 0
        nexiting = 0
        ndone = 0
        nfault = 0

        key = "lcr%d" % lab.id
        val = cache.get(key)
        if val is None:
            msg = "MISS key: %s" % key
            logging.debug(msg)
            # key not known so set to current count
            val = jobs.filter(label=lab, state='created').count()
            added = cache.add(key, val, lifetime)
            if added:
                msg = "Added DB count for key %s : %d" % (key, val)
                logging.warn(msg)
            else:
                msg = "Failed to add DB count for key %s : %d" % (key, val)
                logging.warn(msg)
        ncreated = val

        key = "lrn%d" % lab.id
        val = cache.get(key)
        if val is None:
            msg = "MISS key: %s" % key
            logging.debug(msg)
            # key not known so set to current count
            val = jobs.filter(label=lab, state='running').count()
            added = cache.add(key, val, lifetime)
            if added:
                msg = "Added DB count for key %s : %d" % (key, val)
                logging.warn(msg)
            else:
                msg = "Failed to add DB count for key %s : %d" % (key, val)
                logging.warn(msg)
        nrunning = val

        key = "lex%d" % lab.id
        val = cache.get(key)
        if val is None:
            msg = "MISS key: %s" % key
            logging.debug(msg)
            # key not known so set to current count
            val = jobs.filter(label=lab, state='exiting').count()
            added = cache.add(key, val, lifetime)
            if added:
                msg = "Added DB count for key %s : %d" % (key, val)
                logging.warn(msg)
            else:
                msg = "Failed to add DB count for key %s : %d" % (key, val)
                logging.warn(msg)
        nexiting = val

        key = "ldn%d" % lab.id
        val = cache.get(key)
        if val is None:
            msg = "MISS key: %s" % key
            logging.debug(msg)
            # key not known so set to current count
            val = jobs.filter(label=lab, state='done').count()
            added = cache.add(key, val, lifetime)
            if added:
                msg = "Added DB count for key %s : %d" % (key, val)
                logging.warn(msg)
            else:
                msg = "Failed to add DB count for key %s : %d" % (key, val)
                logging.warn(msg)
        ndone = val

        statcr = 'pass'
        if ncreated >= 500:
            statcr = 'warn'

        statdone = 'pass'
        if ndone <= 0:
            statdone = 'warn'

        statfault = 'pass'
        if nfault >= 50:
            statfault = 'hot'

        delayed = datetime.now(pytz.utc) - timedelta(minutes=5)
        stale = datetime.now(pytz.utc) - timedelta(minutes=30)
        activity = 'ok'
        if delayed > lab.last_modified:
            activity = 'warn'
        if stale > lab.last_modified:
            activity = 'stale'
    
        row = {
            'label' : lab,
            'pandaq' : lab.batchqueue,
            'ncreated' : ncreated,
            'nrunning' : nrunning,
            'nexiting' : nexiting,
            'ndone' : ndone,
            'nfault' : nfault,
            'statcr' : statcr,
            'statdone' : statdone,
            'statfault' : statfault,
            'activity' : activity,
            }

        rows.append(row)

    context = {
            'rows' : rows,
            'jobs' : jobs,
            'pandaqs' : pandaqs,
            'factory' : f,
            }

    return render_to_response('mon/factory.html', context)

def pandaq(request, qid, p=1):
    """
    Rendered view of panda queue for all factories
    """

    q = get_object_or_404(BatchQueue, id=qid)

    labels = Label.objects.filter(batchqueue=q)
    dt = datetime.now(pytz.utc) - timedelta(hours=1)
    dtdead = datetime.now(pytz.utc) - timedelta(days=10)
    # factories with labels serving selected batchqueue
    fs = Factory.objects.filter(label__in=labels)

    rows = []
    for lab in labels:
        if lab.last_modified < dtdead: continue
        row = {}
        ncreated = 0
        nsubmitted = 0
        nrunning = 0
        nexiting = 0
        ndone = 0
        nfault = 0
        jobs = Job.objects.filter(label=lab)
        ncreated = jobs.filter(state='created').count()
        nrunning = jobs.filter(state='running').count()
        nexiting = jobs.filter(state='exiting').count()
        ndone = jobs.filter(state='done').count()
        nfault = jobs.filter(state='fault').count()
        nmiss = jobs.filter(state='done', result=20).count()
    
        row['jobcount'] = {
                'created' : ncreated,
                'running' : nrunning,
                'exiting' : nexiting,
                'done' : ndone,
                'fault' : nfault,
                'miss' : nmiss,
                }

        row['label'] = lab
    
        statdone = 'pass'
        if nexiting == 0:
            statdone = 'fail'
        elif nexiting <= 5:
            statdone = 'warn'
    
        statfault = 'hot'
#        if nfault <= 5:
#            statfault = 'warn'
        if nfault == 0:
            statfault = 'pass'

        row['statdone'] = statdone
        row['statfault'] = statfault

        dtwarn = datetime.now(pytz.utc) - timedelta(minutes=5)
        dtstale = datetime.now(pytz.utc) - timedelta(minutes=10)

        active = 'stale'
        if lab.last_modified > dtstale:
            active = 'warn'
        if lab.last_modified > dtwarn:
            active = 'pass'

        row['activity'] = active

        rows.append(row)

    pages = Paginator(Job.objects.filter(label__batchqueue=qid).order_by('-last_modified'), 100)
    jobs = Job.objects.filter(label__batchqueue=qid).order_by('-last_modified')[:100]

    context = {
            'pandaq' : q,
            'rows' : rows,
            'jobs' : jobs,
            'pages' : pages,
            'page' : pages.page(p),
            }

    return render_to_response('mon/pandaq.html', context)

def offline(request):

    context = {}
    return render_to_response('mon/offline.html', context)

# APIv1
def rn(request, fid, cid):
    """
    Handle 'rn' signal from a running job
    """
    stat = 'apfmon.rn'
    start = time.time()

    try:
        f = Factory.objects.get(name=fid)
    except Factory.DoesNotExist:
        msg = "Factory %s not found" % fid
        logging.debug(msg)
        content = "Bad request"
        return HttpResponseBadRequest(content, mimetype="text/plain")

    try:
        jid = ':'.join((fid, cid))
        j = Job.objects.get(jid=jid)
    except Job.DoesNotExist, e:
        msg = "RN unknown job: %s_%s" % (f, cid)
#PAL - pending fix for apfv2        logging.warn(msg)
        content = "Fine"
        return HttpResponseBadRequest(content, mimetype="text/plain")

    msg = None

    if j.state == 'created':
        msg = "%s -> RUNNING" % j.state
        element = "%f %s %s" % (time.time(), request.META['REMOTE_ADDR'], msg)
        red.rpush(j.jid, element)
        red.expire(j.jid, expire7days)

        j.state = 'running'
        if j.flag:
            j.flag = False
            msg = "RUNNING now, flag cleared"
            element = "%f %s %s" % (time.time(), request.META['REMOTE_ADDR'], msg)
            red.rpush(j.jid, element)
        j.save()
        c2r = time.time() - time.mktime(j.created.timetuple())
        name = str(j.label.name).replace(':','_')
        stat = 'apfmon.c2r.%s' % name
        ss.timing(stat,int(c2r))

    else:
        msg = "%s -> RUNNING (WARN: state not CREATED)" % j.state
        element = "%f %s %s" % (time.time(), request.META['REMOTE_ADDR'], msg)
        red.rpush(j.jid, element)
        red.expire(j.jid, expire2days)

        j.state = 'running'
        j.flag = True
        j.save()

    elapsed = time.time() - start
    ss.timing(stat,int(elapsed))
    return HttpResponse("OK", mimetype="text/plain")

# APIv1
def ex(request, fid, cid, sc=None):
    """
    Handle 'ex' signal from exiting wrapper
    """
    stat = 'apfmon.ex'
    start = time.time()
    
    try:
        f = Factory.objects.get(name=fid)
    except Factory.DoesNotExist:
        msg = "Factory %s not found" % fid
        logging.debug(msg)
        content = "Bad request"
        return HttpResponseBadRequest(content, mimetype="text/plain")

    jid = ':'.join((fid,cid))
    try:
        j = Job.objects.get(jid=jid)
    except Job.DoesNotExist, e:
        msg = "EX unknown Job: %s:%s" % (f, cid)
#PAL - pending fix for apfv2        logging.warn(msg)
        return HttpResponseBadRequest('Fine', mimetype="text/plain")
    
    msg = None

    if j.state in ['done', 'fault']:
        msg = "Terminal: %s, no state change allowed." % j.state
        element = "%f %s %s" % (time.time(), request.META['REMOTE_ADDR'], msg)
        red.rpush(j.jid, element)
        red.expire(j.jid, expire2days)

        j.flag = True
        j.save()

    elif j.state == 'running':
        msg = "%s -> EXITING statuscode: %s" % (j.state, sc)
        element = "%f %s %s" % (time.time(), request.META['REMOTE_ADDR'], msg)
        red.rpush(j.jid, element)
        red.expire(j.jid, expire2days)

        j.state = 'exiting'
        if sc:
            j.result = (sc)
        else:
            j.flag = True
        j.save()

    else:
        msg = "%s -> EXITING STATUSCODE: %s (WARN: state not RUNNING)" % (j.state, sc)
        element = "%f %s %s" % (time.time(), request.META['REMOTE_ADDR'], msg)
        red.rpush(j.jid, element)

        j.state = 'exiting'
        if sc:
            j.result = int(sc)
        j.flag = True
        j.save()


    elapsed = time.time() - start
    ss.timing(stat,int(elapsed))
    return HttpResponse("OK", mimetype="text/plain")

# APIv1
def action(request):
    """
    Update the latest factory actions
    """

    nick = request.POST.get('nick', None)
    fid = request.POST.get('fid', None)
    label = request.POST.get('label', None)
    txt = request.POST.get('msg', None)
    ip = request.META['REMOTE_ADDR']
    
    if not (nick and fid and label and txt):
        content = "Bad request"
        return HttpResponseBadRequest(content, mimetype="text/plain")

    txt = txt[:140]

    pq = get_object_or_404(BatchQueue, name=nick)
    f = get_object_or_404(Factory, name=fid)
    lab = get_object_or_404(Label, name=label, fid=f)

    try:
        lab.msg = txt
        lab.save()
    except Exception, e:
        msg = "Failed to update label: %s" % lab
        print msg, e
        return HttpResponseBadRequest(msg, mimetype="text/plain")

    return HttpResponse("OK", mimetype="text/plain")

# APIv1
@cache_page(60 * 10)
def stats(request):
    """
    WTF
    """
    labels = Label.objects.all()

    lifetime = 300
    rows = []
    for l in labels:
        print l
        key = "ldn%d" % l.id
        val = cache.get(key)
        if val is None:
            msg = "MISS key: %s" % key
            logging.debug(msg)
            # key not known so set to current count
            val = Job.objects.filter(label=l, state='done').count()
            added = cache.add(key, val, lifetime)
            if added:
                msg = "Added DB count for key %s : %d" % (key, val)
                logging.debug(msg)
            else:
                msg = "Failed to add DB count for key %s : %d" % (key, val)
                logging.debug(msg)
        ndone = val

        key = "lft%d" % l.id
        val = cache.get(key)
        if val is None:
            msg = "MISS key: %s" % key
            logging.debug(msg)
            # key not known so set to current count
            val = Job.objects.filter(label=l, state='fault').count()
            added = cache.add(key, val, lifetime)
            if added:
                msg = "Added DB count for key %s : %d" % (key, val)
                logging.debug(msg)
            else:
                msg = "Failed to add DB count for key %s : %d" % (key, val)
                logging.debug(msg)
        nfault = val

        dt = datetime.now(pytz.utc)
        date = "%d-%02d-%02d" % (dt.year, dt.month, dt.day)
        # these need to come from Factory info
        out = "%s/%s/%s/%s.out"
        err = "%s/%s/%s/%s.err"
        log = "%s/%s/%s/%s.log"

        dir = str(l.name).translate(string.maketrans('/:','__'))

        logurl = "%s/%s/%s/" % (l.fid.url, date, dir)


        url = l.fid.url
        nhit = Job.objects.filter(state='done', label=l, result=0).count()
        nmiss = Job.objects.filter(state='done', label=l, result=20).count()
        row = {
            'label' : l.name,
            'labelid' : l.id,
            'factory' : l.fid.name,
            'factoryid' : l.fid.id,
            'factoryver' : l.fid.version,
            'pandaq' : l.batchqueue.name,
            'pandaqid' : l.batchqueue.id,
            'ndone' : ndone,
            'nfault' : nfault,
            'nhit' : nhit,
            'nmiss' : nmiss,
            'logurl' : logurl,
            'timestamp' : datetime.now(pytz.utc).strftime('%F %H:%M:%S UTC'),
            }
        rows.append(row)

    context = {
        'rows' : rows,
        }

    return HttpResponse(json.dumps(rows, sort_keys=True, indent=2), mimetype="application/json")

# UI
@cache_page(60 * 1)
def index(request):
    """
    Rendered view of front page which shows a table of activity
    for each factories
    """
    jobs = Job.objects.all()
    dtfail = datetime.now(pytz.utc) - timedelta(days=10)
    dterror = datetime.now(pytz.utc) - timedelta(hours=1)
    dtwarn = datetime.now(pytz.utc) - timedelta(minutes=20)

    factories = Factory.objects.all().order_by('name')

    rows = []
    for f in factories:
        if f.last_modified < dtfail: continue

        # this 'active' string map to a html classes
        active = 'text-error'
        if f.last_modified > dterror:
            active = 'text-warning'
        if f.last_modified > dtwarn:
            active = ''
        row = {
            'factory' : f,
            'active' : active,
            'activity' : f.last_ncreated,
            }

        rows.append(row)

    status = red.get('apfmon:status')

    context = {
            'rows' : rows,
            'clouds' : CLOUDLIST,
            'status' : status,
            }

    return render_to_response('mon/index.html', context)

# UI
def cloud(request, name):
    """
    Rendered view of Cloud page showing table of Sites in this cloud.
    """
    sites = Site.objects.filter(cloud=name)

    labels = Label.objects.filter(batchqueue__wmsqueue__site__cloud=name)
    dtwarn = datetime.now(pytz.utc) - timedelta(minutes=20)

    factive = []
    finactive = []
    for label in labels:
        if label.fid not in factive + finactive:
            if label.fid.last_modified > dtwarn:
                factive.append(label.fid)
            else:
                finactive.append(label.fid)

    nrunning = 0
    rows = []

    for site in sites:
        pandaqs = BatchQueue.objects.filter(wmsqueue__site=site)
        for pandaq in pandaqs:
            elogmatch = ELOGREGEX.match(pandaq.comment)
            ggusmatch = GGUSREGEX.match(pandaq.comment)
            savmatch = SAVANNAHREGEX.match(pandaq.comment)
            url = prefix = suffix = None
            if elogmatch:
                prefix = elogmatch.group(1)
                suffix = elogmatch.group(2)
                url = ELOGURL % suffix
            elif ggusmatch:
                prefix = ggusmatch.group(1)
                suffix = ggusmatch.group(2)
                url = GGUSURL % suffix 
            elif savmatch:
                prefix = savmatch.group(1)
                suffix = savmatch.group(2)
                url = SAVANNAHURL % suffix

            jobs = Job.objects.filter(label__batchqueue=pandaq)

            # dull messages
            cssclass = pandaq.state 
#            if pandaq.type in ['SPECIAL_QUEUE']:
#                cssclass = 'muted'

            dull = [
                    'HC.Blacklist.set.online',
                    'HC.Blacklist.set.test',
                    ]
            msgclass = cssclass
            if pandaq.comment in dull:
                msgclass = 'muted'

            row = {
                    'site'     : site,
                    'url'      : url,
                    'prefix'   : prefix,
                    'suffix'   : suffix,
                    'pandaq'   : pandaq,
                    'class'    : cssclass,
                    'msgclass' : msgclass,
                    }
            rows.append(row)

    context = {
            'factive' : factive,
            'finactive' : finactive,
            'cloud' : name,
            'sites' : sites,
            'rows' : rows,
            }


    return render_to_response('mon/cloud.html', context)

def testtimeline(request):

    jobs = Job.objects.filter(label__name='UKI-NORTHGRID-LANCS-HEP-abaddon-cream').order_by('-last_modified')

    context = {
            'jobs' : jobs[:10],
            }

    return render_to_response('mon/test.html', context)

# UI wtf
@cache_page(60 * 10)
def queues(request):
    """
    Rendered view of all queues, all factories.
    """

    # cache lifetime for pandaq state counts
    lifetime = 300

    clouds = Site.objects.values_list('cloud', flat=True).order_by('cloud').distinct()
    pandaqs = BatchQueue.objects.filter().order_by('wmsqueue__site__cloud','name')
    dt = datetime.now(pytz.utc) - timedelta(hours=1)
    jobs = Job.objects.all()

    cloudlist = []
    for cloud in clouds:
        npq = BatchQueue.objects.filter(wmsqueue__site__cloud=cloud).count()
        cloudlist.append({'name' : cloud, 'npq' : npq})

    rows = []
    for pandaq in pandaqs:
        labs = Label.objects.filter(batchqueue=pandaq)
        nactive = 0
        ndone = 0
        nfault = 0
        
        # ACTIVE job count from cache
        key = "pq%d%s" % (pandaq.id, 'astate')
        val = cache.get(key)
        if val is None:
            # key not known so set to current count
            msg = "MISS key: %s" % key
            logging.debug(msg)
            jobs = jobs.filter(label__batchqueue=pandaq)
            jobs = jobs.filter(state__in=['created','running','exiting'])
            val = jobs.count()
            added = cache.add(key, val, lifetime)
            if added:
                msg = "Added DB count for key %s : %d" % (key, val)
                logging.warn(msg)
            else:
                msg = "queues() failed to add DB count for key %s : %d" % (key, val)
                logging.warn(msg)
        nactive = val

        # DONE job count from cache
        key = "pq%d%s" % (pandaq.id, 'dstate')
        val = cache.get(key)
        if val is None:
            # key not known so set to current count
            msg = "MISS key: %s" % key
            logging.debug(msg)
            val = jobs.filter(label__batchqueue=pandaq, state="done").count()
            added = cache.add(key, val, lifetime)
            if added:
                msg = "Added DB count for key %s : %d" % (key, val)
                logging.warn(msg)
            else:
                msg = "Failed to add DB count for key %s : %d" % (key, val)
                logging.warn(msg)
        ndone = val

        # FAULT job count from cache
        key = "pq%d%s" % (pandaq.id, 'fstate')
        val = cache.get(key)
        if val is None:
            # key not known so set to current count
            msg = "MISS key: %s" % key
            logging.debug(msg)
            val = jobs.filter(label__batchqueue=pandaq, state="fault").count()
            added = cache.add(key, val, lifetime)
            if added:
                msg = "Added DB count for key %s : %d" % (key, val)
                logging.warn(msg)
            else:
                msg = "Failed to add DB count for key %s : %d" % (key, val)
                logging.warn(msg)
        nfault = val

        row = {
            'pandaq'     : pandaq,
            'nactive'    : nactive,
            'ndone'      : ndone,
            'nfault'     : nfault,
            }

        rows.append(row)

    fids = []       
    context = {
            'clouds'    : clouds,
            'rows'      : rows,
            'factories' : fids,
            }

    return render_to_response('mon/queues.html', context)

# APIv1
def cr(request):
    """
    Create the Job, expect data format is:
    (cid, nick, fid, label)
    """
    stat = 'apfmon.cr'
    start = time.time()

    if 'CONTENT_LENGTH' in request.META.keys():
        length = request.META['CONTENT_LENGTH']
        msg = "cr content length: %s" % length
        logging.debug(msg)
        ss.gauge('apfmon.length.cr',length)
    else:
        msg = 'No CONTENT_LENGTH in request'
        logging.debug(msg)

    ip = request.META['REMOTE_ADDR']
    jdecode = json.JSONDecoder()

    raw = request.POST.get('data', None)

    if not raw:
        msg = 'No POST data found'
        logging.error(msg)
        content = "Bad request, no POST data found"
        return HttpResponseBadRequest(content, mimetype="text/plain")

    try:
        data = jdecode.decode(raw)
        ncreated = len(data)
        msg = "Number of jobs in JSON data: %d (%s)" % (ncreated, ip)
        logging.debug(msg)
        ss.gauge('apfmon.apiv1.jsoncount',ncreated)
    except:
        msg = 'Error decoding POST json data'
        logging.error(msg)
        content = "Bad request"
        return HttpResponseBadRequest(content, mimetype="text/plain")

    for d in data:
        cid = d[0]
        nick = d[1]
        fid = d[2]
        label = d[3]
    
        pq, created = BatchQueue.objects.get_or_create(name=nick)
        if created:
            msg = 'PAL FID:%s, BatchQueue auto-created, no siteid: %s' % (fid,nick)
            logging.error(msg)
            pq.save()
    
        f, created = Factory.objects.get_or_create(name=fid, defaults={'ip':ip})
        if created:
            msg = "PAL Factory auto-created: %s" % fid
            logging.error(msg)
        f.last_ncreated = ncreated
        f.save()
    
        try: 
            lab = Label.objects.get(name=label, fid=f)
        except:
            lab = Label(name=label, fid=f, batchqueue=pq)
            lab.save()
            msg = "PAL Label cr() auto-created: %s" % lab
            logging.error(msg)
            
        try:
            jid = ':'.join((f.name,cid))
            j = Job(jid=jid, cid=cid, state='created', label=lab)
            j.save()

            # this awesome section populates a ring-counter with the number
            # of jobs per label over the last 2hrs (24 * 5 min buckets)
            labelkey = ':'.join(('jobcount',f.name,lab.name))
            factorykey = ':'.join(('jobcount',f.name))
            bucket = '%s' % math.floor((time.time() % span) / interval)
            next1bucket = '%s' % math.floor(((time.time()+interval) % span) / interval)
            next2bucket = '%s' % math.floor(((time.time()+(2*interval)) % span) / interval)
            pipe = red.pipeline()
            pipe.hincrby(labelkey, bucket, 1)
            pipe.hincrby(factorykey, bucket, 1)
            pipe.hmset(labelkey, {next1bucket:0, next2bucket:0})
            pipe.hmset(factorykey, {next1bucket:0, next2bucket:0})
            pipe.expire(labelkey, expire3hrs)
            pipe.expire(factorykey, expire3hrs)
            pipe.execute()

        except Exception, e:
            msg = "Failed to create cr(): fid=%s cid=%s state=created label=%s jid=%s" % (f,cid,lab,jid)
            logging.error(e)
            logging.error(msg)
            return HttpResponseBadRequest(msg, mimetype="text/plain")
    
    elapsed = time.time() - start
    ss.timing(stat,int(elapsed))

    txt = 'job' if len(data) == 1 else 'jobs'
    context = 'Received %d %s' % (len(data), txt) 
    return HttpResponse(context, mimetype="text/plain")

# APIv1
def helo(request):
    """
    Factory startup messages. Expecting POST with key,value pairs
    Known and used keys:
    factoryId
    monitorURL
    factoryOwner
    baseLogDirUrl
    versionTag

    """
    fid = request.POST.get('factoryId', None)
    owner = request.POST.get('factoryOwner', None)
    url = request.POST.get('baseLogDirUrl', None)
    ip = request.META['REMOTE_ADDR']
    ver = request.POST.get('versionTag', None)

    logging.debug(fid)
    logging.debug(owner)
    logging.debug(url)
    logging.debug(ip)

    if not (fid and owner and url):
        content = "Bad request"
        return HttpResponseBadRequest(content, mimetype="text/plain")

    d = {'ip' : ip,
         'url' : url,
         'email' : owner,
         'last_startup' : datetime.now(pytz.utc),
         'version' : ver,
        }

    f, created = Factory.objects.get_or_create(name=fid, defaults=d)
    if created:
        msg = "Factory auto-created: %s" % fid
        logging.info(msg)
        mail_managers('New factory: %s' % fid,
                      'New factory: %s' % fid,
                      fail_silently=False)


    f.ip = ip
    f.url = url
    f.email = owner
    f.last_startup = datetime.now(pytz.utc)
    f.version = ver
    f.save()

    return HttpResponse("OK", mimetype="text/plain")

# APIv1
def msg(request):
    """
    Update the latest factory messages. Expect data format:
    (nick, fid, label, text)
    """

    ip=request.META['REMOTE_ADDR']

    jdecode = json.JSONDecoder()

    cycle = request.POST.get('cycle', None)
    raw = request.POST.get('data', None)
    
    if raw:
        try:
            data = jdecode.decode(raw)
            msg = "Number of msgs in JSON data: %d (%s)" % (len(data), ip)
            logging.debug(msg)
            if 'CONTENT_LENGTH' in request.META.keys():
                length = request.META['CONTENT_LENGTH']
                msg = "Msg content length: %s" % length
                logging.debug(msg)
                ss.gauge('apfmon.length.msg', length)
            else:
                msg = 'No CONTENT_LENGTH in request'
                logging.debug(msg)
        except:
            msg = 'Error decoding POST json data'
            logging.error(msg)
            content = "Bad request"
            return HttpResponseBadRequest(content, mimetype="text/plain")

        for d in data:
            nick = d[0]
            fid = d[1]
            label = d[2]
            text = d[3]

            txt = text[:140]
        
            ip = request.META['REMOTE_ADDR']
            f = Factory.objects.get(name=fid)
            if cycle:
                f.last_cycle = cycle
                f.save()
        
            try:
                lab = Label.objects.get(name=label, fid=f)

            except:
                msg = "Failed to get msg() Label: %s (fid: %s)" % (label, f)
                logging.warn(msg)
                raise Http404
        
            try:
                lab.msg = txt
                lab.save()
            except Exception, e:
                msg = "Failed to update the label: %s" % lab
                logging.error(msg)
                logging.error(e)
                return HttpResponseBadRequest(msg, mimetype="text/plain")

    return HttpResponse("OK", mimetype="text/plain")

# UI
def help(request):

    context = {}
    return render_to_response('mon/help.html', context)

# APIv1
def search(request):
    """
    Search for a string in pandaqueue, sites, labels.
    """

    query = request.GET.get('q', '')

    # see Simple generic views in django docs

    url = reverse('apfmon.mon.views.query', args=(query,))
    logging.debug(url)
    return HttpResponseRedirect(url)

# UI
def query(request, q=None):
    """
    Search for a string in pandaq
    """
    dt = datetime.now(pytz.utc) - timedelta(days=7)
    
    if q:
        result = red.lpush('apfmon:query', q)
        qset = (
            Q(name__icontains=q) |
            Q(batchqueue__name__icontains=q) |
            Q(batchqueue__wmsqueue__name__icontains=q) |
            Q(batchqueue__wmsqueue__site__name__icontains=q)
            # can add other search params here, eg. SITE name
        )
        labels = Label.objects.filter(qset).order_by('fid', 'name')
        labels = labels.filter(last_modified__gt=dt)
    else:
        labels = []


    context = {
        'labels' : labels,
        'query'  : q,
    }
    return render_to_response('mon/query.html', context)

# UI
def site(request, sid):
    """
    Rendered view of Site page showing table of Pandaqs for this Site
    including stats from all factories
    Note: this is a Site not a PandaSite
    """
    s = get_object_or_404(Site, id=int(sid))
    dt = datetime.now(pytz.utc) - timedelta(days=7)

    # all labels serving this site
    labels = Label.objects.filter(batchqueue__wmsqueue__site=s)
    labels = labels.filter(last_modified__gt=dt)

    rows = []
    for label in labels:
        row = {}
        ncreated = 0
        nsubmitted = 0
        nrunning = 0
        nexiting = 0
        ndone = 0
        nfault = 0
        nmiss = 0
        jobs = Job.objects.filter(label=label)
    
        row['jobcount'] = {
                'created' : ncreated,
                'running' : nrunning,
                'exiting' : nexiting,
                'done' : ndone,
                'fault' : nfault,
                'miss' : nmiss,
                }

        row['label'] = label
        rows.append(row)

    context = {
            'site' : s,
            'rows' : rows,
            }


    return render_to_response('mon/site.html', context)

# UI
def fault(request):
    """
    List Labels which have FAULT jobs
    """

    jobs = Job.objects.filter(state='fault', label__batchqueue__state='online')
    labels = jobs.values('label__id', 'label__batchqueue').annotate(njob=Count('id'))

    rows = []
    for lab in labels:
        lid = lab['label__id']
        nfault = lab['njob']
        if nfault < 1: continue
        nflag = Job.objects.filter(label=lid, flag=True).count()
        totjob = Job.objects.filter(label=lid).count()
        flagfrac = 100 * nflag/totjob
        faultfrac = 100 * nfault/totjob

        try:
            label = Label.objects.get(id=lid)
        except Label.DoesNotExist:
            msg = 'Label does not exist: %s' % lid
            logging.warn(msg)
            continue
        
        row = {
            'label' : label,
            'flagfrac' : flagfrac,
            'faultfrac' : faultfrac,
            'totjob' : totjob,
            }
        rows.append(row)

    # find panda queues only being serviced by one factory
    qlist = Label.objects.values('batchqueue__name','batchqueue__id').annotate(n=Count('fid'))
    sololist = []
    for q in qlist:
        if q['n'] == 1: sololist.append(q)

    sortedrows = sorted(rows, key=itemgetter('flagfrac'), reverse=True) 

    # find orphan labels (not associated with a batchqueue)
    orphans = Label.objects.filter(batchqueue=None)

    context = {
        'rows'     : sortedrows[:20],
        'sololist' : sololist,
        'orphans'  : orphans,
        }

    return render_to_response('mon/fault.html', context)

# UI
def label(request, lid, p=1):
    """
    Rendered view of a single Label with job details
    """

    lab = get_object_or_404(Label, id=lid)

    dt = datetime.now(pytz.utc) - timedelta(hours=1)
    # factories with labels serving selected pandaq

    ncreated = 0
    nsubmitted = 0
    nrunning = 0
    nexiting = 0
    ndone = 0
    nfault = 0

    jobs = Job.objects.filter(label=lab)
    ncreated = jobs.filter(state='created').count()
    nrunning = jobs.filter(state='running').count()
    nexiting = jobs.filter(state='exiting').count()
    ndone = jobs.filter(state='done').count()
    nfault = jobs.filter(state='fault').count()
    nmiss = jobs.filter(state='done', result=20).count()
    
    counts = {
            'created' : ncreated,
            'running' : nrunning,
            'exiting' : nexiting,
            'done' : ndone,
            'fault' : nfault,
            'miss' : nmiss,
            }

    activewarn = datetime.now(pytz.utc) - timedelta(minutes=5)
    activeerror = datetime.now(pytz.utc) - timedelta(minutes=10)
    status = 'ok'
    if activewarn > lab.last_modified:
        status = 'warn'
    if activeerror > lab.last_modified:
        status = 'fail'

    pages = Paginator(Job.objects.filter(label=lid).order_by('-last_modified'), 200)
    jobs = Job.objects.filter(label=lid).order_by('-last_modified')[:200]
    
    # make an ordered jobcount list from the redis hash
    lid = ':'.join((lab.fid.name,lab.name))
    key = ':'.join(('jobcount',lid))
    n = span / interval
    buckets = []
    for i in range(n):
        t = time.time() - (i * interval)
        buckets.append(math.floor((t % span) / interval))
    activity = red.hmget(key, buckets)
    def makezero(value): return int(0 if value is None else value)
    activity = map(makezero, activity)
    activity.reverse()

    context = {
            'label'    : lab,
            'lid'      : lid,
            'jobs'     : jobs,
            'pages'    : pages,
            'page'     : pages.page(p),
            'status'   : status,
            'activity' : activity,
            'counts'   : counts,
            }

    return render_to_response('mon/label.html', context)

def test(request):

    jobs = Job.objects.all()

    context = {}
    return render_to_response('mon/test.html', context)

def debug(request):
    """
    Rendered view of selected Jobs
    """

    dt = datetime.now(pytz.utc) - timedelta(minutes=60)
    fault = Job.objects.filter(last_modified__gt=dt, state__name='FAULT')
    done = Job.objects.filter(last_modified__gt=dt, state__name='DONE').filter(result=0)
    flagged = Job.objects.filter(last_modified__gt=dt, flag=True)
    havejob = done.filter(result=0)

    dt = datetime.now(pytz.utc) - timedelta(hours=96)
    ancient = Job.objects.filter(created__lt=dt).order_by('created').exclude(state__name__in=['FAULT','DONE'])

    context = {
                'ancient' : ancient,
                'flagged' : flagged,
                'fault' : fault,
                'done' : done,
                'havejob' : havejob,
                }

    return render_to_response('mon/debug.html', context)

