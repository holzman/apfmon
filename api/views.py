from atl.mon.models import State
from atl.mon.models import Factory
from atl.mon.models import Job
from atl.mon.models import Label
from atl.mon.models import Message
#from atl.mon.models import Pandaid

from atl.kit.models import Cloud
from atl.kit.models import Tag
from atl.kit.models import Site
from atl.kit.models import PandaQueue
from atl.kit.models import PandaSite

#import csv
import logging
import pytz
#import re
import statsd
#import string
#import sys
#from time import time
#from operator import itemgetter
from datetime import timedelta, datetime
from django.shortcuts import redirect, render_to_response, get_object_or_404
#from django.db.models import Count
#from django.db.models import Q
from django.http import HttpResponse, Http404, HttpResponseBadRequest
from django.http import HttpResponseRedirect
from django.conf import settings
from django.core.cache import cache
#from django.views.decorators.cache import cache_page
from django.core.mail import mail_managers
#from django.core.paginator import Paginator, InvalidPage, EmptyPage
from django.core.serializers.json import DjangoJSONEncoder
from django.core.urlresolvers import reverse
from django.core.exceptions import MultipleObjectsReturned

try:
    import json as json
except ImportError, err:
    logging.error('Cannot import json, using simplejson')
    import simplejson as json

ss = statsd.StatsClient(settings.GRAPHITE['host'], settings.GRAPHITE['port'])

def job(request, id):
    """
    Handle requests from /jobs/{id} resource

    GET:
    Return a specific job including list of messages

    POST:
    Update the Job using data in the querystring
    state : either 'running' or 'exiting'
    ids   : comma separated list of pandaids (not implemented)
    """

    ip = request.META['REMOTE_ADDR']

    job = get_object_or_404(Job, jid=id)

    if request.method == 'GET':
        messages = Message.objects.filter(job=job).values('client',
                                   'msg', 'received')

        jobfields = ('jid', 'cid', 'fid__name', 'created', 'flag',
                   'label__name', 'last_modified', 'pandaq__name',
                   'result', 'state__name')
        j = Job.objects.filter(jid=id).values(*jobfields)[0]

        j['messages'] = list(messages)

        response = HttpResponse(json.dumps(j, 
                                cls=DjangoJSONEncoder,
                                sort_keys=True,
                                indent=2),
                                mimetype="application/json")
        location = "/api/jobs/%s" % job.jid
        response['Location'] = location
        return response
        

    if request.method == 'POST':
        newstate = request.POST.get('state', None)

        if newstate == 'running':
            if job.state.name != 'CREATED':
                msg = "Invalid state transition, %s->%s" % (
                                                job.state.name, newstate.upper())
                return HttpResponseBadRequest(msg, mimetype="text/plain")
            job.state = State.objects.get(name='RUNNING')
            job.save()
            response = HttpResponse(mimetype="text/plain")
            location = "/api/jobs/%s" % job.jid
            response['Location'] = location
            return response

        elif newstate == 'exiting':
            if job.state.name != 'RUNNING':
                msg = "Invalid state transition: %s->%s" % (
                                                job.state.name, newstate.upper())
                return HttpResponseBadRequest(msg, mimetype="text/plain")

            job.state = State.objects.get(name='EXITING')
            job.save()
            return HttpResponse(mimetype="text/plain")

        else:
            msg = "Invalid data: %s" % dict(request.POST)
            return HttpResponseBadRequest(msg, mimetype="text/plain")

    if request.method == 'DELETE':
        if ip == '127.0.0.1':
            job.delete()
            return HttpResponse(mimetype="text/plain")
        else: 
            context = "Remote deletion is forbidden"
            return HttpResponseForbidden(context, mimetype="text/plain")
            

    context = 'HTTP method not supported: %s' % request.method
    return HttpResponse(context, mimetype="text/plain")

def jobs(request):
    """
    Handle requests from /jobs resource.

    GET:
    Return a list of jobs refined by zero or more URL parameters fid,state,label

    PUT:
    Create the Job, expect data format is a JSON encoded list of dicts
    with the following keys:
    cid     : unique id of job, usually condorid but can be anything 
    nick    : panda queue name
    factory : factory name
    label   : factory label for each queue (name of section in factory config)
    queue   : computing resource endpoint
    localqueue : local queue at the endpoint
    """

    ip = request.META['REMOTE_ADDR']

    if request.method == 'PUT':

        msg = "RAW REQUEST: %s %s %s" % (request.method, ip, request.body)
        logging.debug(msg)

        try:
            jobs = json.loads(request.body)
        except ValueError, e:
            msg = str(e)
            return HttpResponseBadRequest(msg, mimetype="text/plain")

        msg = "Number of jobs in JSON data: %d (%s)" % (len(jobs), ip)
        logging.debug(msg)

        nfailed = 0
        ncreated = 0
        for job in jobs:
            nick = job['nick']
            factory = job['factory']
            label = job['label']
            cid = job['cid']
            queue = job.get('queue', None)
            localqueue = job.get('localqueue', None)
            
            f, created = Factory.objects.get_or_create(name=factory, defaults={'ip':ip})
            if created:
                msg = "Factory auto-created: %s" % factory
                logging.warn(msg)
            f.last_ncreated = len(jobs)
            f.save()

            pq, created = PandaQueue.objects.get_or_create(name=nick)
            if created:
                msg = 'PandaQueue auto-created: %s (%s)' % (nick,factory)
                logging.warn(msg)
                pq.save()
    
            try: 
                lab, created = Label.objects.get_or_create(name=label, fid=f, pandaq=pq)
            except MultipleObjectsReturned,e:
                msg = "Multiple objects - apfv2 issue?"
                logging.warn(msg)
                msg = "Multiple objects error"
                return HttpResponseBadRequest(msg, mimetype="text/plain")
            if created:
                msg = "Label auto-created: %s" % label
                logging.debug(msg)
    
            if lab.queue != queue:
                lab.queue = queue
                lab.save()
            if lab.localqueue != localqueue:
                lab.queue = localqueue
                lab.save()

            try:
                state = State.objects.get(name='CREATED')
                jid = ':'.join((f.name,cid))
                j = Job(jid=jid, cid=cid, fid=f, state=state, pandaq=pq, label=lab)
                j.save()
                ncreated += 1

                key = "fcr%d" % f.id
                try:
                    val = cache.incr(key)
                except ValueError:
                    msg = "MISS key: %s" % key
                    logging.warn(msg)
                    # key not known so set to current count
                    val = Job.objects.filter(fid=f, state=state).count()
                    added = cache.add(key, val)
                    if added:
                        msg = "Added DB count for key %s : %d" % (key, val)
                        #logging.warn(msg)
                    else:
                        msg = "Failed to incr key: %s" % key
                        logging.warn(msg)

#                if not val % 1000:
                msg = "memcached key:%s val:%d" % (key, val)
                logging.warn(msg)
            except Exception, e:
                msg = "Failed to create: fid=%s cid=%s state=%s pandaq=%s label=%s" % (f,jid,state,pq,lab)
                logging.error(msg)
                logging.error(e)
                nfailed += 1

        txt = 'job' if len(jobs) == 1 else 'jobs'
        context = 'Created %d/%d %s, %d not created' % (ncreated, len(jobs), txt, nfailed)
        status = 201 if ncreated else 200
        return HttpResponse(context, status=status, mimetype="text/plain")

    if request.method == 'GET':
        jobs = Job.objects.all()
        factory = request.GET.get('factory', None)
        label = request.GET.get('label', None)
        site = request.GET.get('site', None)
        state = request.GET.get('state', None)
        

        if factory:
            jobs = jobs.filter(fid__name=factory)

        if label:
            jobs = jobs.filter(label__name=label)

        if state:
            jobs = jobs.filter(state__name=state.upper())

# limit
# offset
        return HttpResponse(json.dumps(list(jobs.values('jid','state__name')), 
                            cls=DjangoJSONEncoder,
                            sort_keys=True,
                            indent=2),
                            mimetype="application/json")

    context = 'HTTP method not supported: %s' % request.method
    return HttpResponse(context, status=405, mimetype="text/plain")

def label(request, id=None):
    """
    Handle requests for the /labels/{id} resource 

    GET:
    Return a label identified by {id} where id = factory:labelname

    POST:
    status : message from factory updating status of [queue]
           : truncated to 140 chars
    """

    ip = request.META['REMOTE_ADDR']

    try:
        factory, name = id.split(':')
    except ValueError:
        msg = "Invalid %s request" % request.method
        return HttpResponseBadRequest(msg, mimetype="text/plain")
        
    label = get_object_or_404(Label, name=name, fid__name=factory)

    if request.method == 'GET':
        fields = ('id','name','fid__name','msg','last_modified','queue', 'localqueue')

        lab = Label.objects.filter(name=name, fid__name=factory).values(*fields)[0]

        return HttpResponse(json.dumps(lab,
                            cls=DjangoJSONEncoder,
                            sort_keys=True,
                            indent=2),
                            mimetype="application/json")

    if request.method == 'POST':
        status = request.POST.get('status', None)
        if not status:
            msg = "Invalid data: %s" % dict(request.POST)
            return HttpResponseBadRequest(msg, mimetype="text/plain")

        label.msg = status[:140]
        label.save()
        response = HttpResponse(mimetype="text/plain")
        location = "/api/labels/%s" % ':'.join((factory,name))
        response['Location'] = location
        return response

    context = "HTTP method not supported: %s" % request.method
    return HttpResponse(context, status=405, mimetype="text/plain")


def labels(request):
    """
    Handle requests for the /labels resource 

    GET:
    Return a list of all labels

    PUT:
    Create or update a collection of Labels. Accepts a JSON encoded list of qcl
    """

    ip = request.META['REMOTE_ADDR']

    if request.method == 'PUT':

        msg = "RAW REQUEST: %s %s %s" % (request.method, ip, request.body)
        logging.debug(msg)

        mail_managers('PUT /api/labels from: %s' % ip,
                      'PUT /api/labels from: %s' % ip,
                      fail_silently=False)

        context = "HTTP method not supported: %s" % request.method
        return HttpResponse(context, status=405, mimetype="text/plain")



#        try:
#            jobs = json.loads(request.body)
#        except ValueError, e:
#            msg = str(e)
#            return HttpResponseBadRequest(msg, mimetype="text/plain")
#
#        msg = "Number of jobs in JSON data: %d (%s)" % (len(jobs), ip)
#        logging.debug(msg)
#
#        nfailed = 0
#        ncreated = 0
#        for job in jobs:
#            nick = job['nick']
#            factory = job['factory']
#            label = job['label']
#            cid = job['cid']
#
#            pq, created = PandaQueue.objects.get_or_create(name=nick)
#            if created:
#                msg = 'PandaQueue auto-created: %s (%s)' % (nick,factory)
#                logging.warn(msg)
#                pq.save()
#    
#            f, created = Factory.objects.get_or_create(name=factory, defaults={'ip':ip})
#            if created:
#                msg = "Factory auto-created: %s" % factory
#                logging.warn(msg)
#            f.last_ncreated = len(jobs)
#            f.save()
#    
#            try: 
#                l, created = Label.objects.get_or_create(name=label, fid=f, pandaq=pq)
#            except MultipleObjectsReturned,e:
#                msg = "Multiple objects - apfv2 issue?"
#                logging.warn(msg)
#                msg = "Multiple objects error"
#                return HttpResponseBadRequest(msg, mimetype="text/plain")
#            if created:
#                msg = "Label auto-created: %s" % label
#                logging.debug(msg)
#    
#            try:
#                state = State.objects.get(name='CREATED')
#                jid = ':'.join((f.name,cid))
#                j = Job(jid=jid, cid=cid, fid=f, state=state, pandaq=pq, label=l)
#                j.save()
#                ncreated += 1
#
#                key = "fcr%d" % f.id
#                try:
#                    val = cache.incr(key)
#                except ValueError:
#                    msg = "MISS key: %s" % key
#                    logging.warn(msg)
#                    # key not known so set to current count
#                    val = Job.objects.filter(fid=f, state=state).count()
#                    added = cache.add(key, val)
#                    if added:
#                        msg = "Added DB count for key %s : %d" % (key, val)
#                        #logging.warn(msg)
#                    else:
#                        msg = "Failed to incr key: %s" % key
#                        logging.warn(msg)
#
##                if not val % 1000:
#                msg = "memcached key:%s val:%d" % (key, val)
#                logging.warn(msg)
#            except Exception, e:
#                msg = "Failed to create: fid=%s cid=%s state=%s pandaq=%s label=%s" % (f,jid,state,pq,l)
#                logging.error(msg)
#                logging.error(e)
#                nfailed += 1
#
#        txt = 'job' if len(jobs) == 1 else 'jobs'
#        context = 'Created %d/%d %s, %d not created' % (ncreated, len(jobs), txt, nfailed)
#        status = 201 if ncreated else 200
#        return HttpResponse(context, status=status, mimetype="text/plain")

    if request.method == 'GET':
        labels = Label.objects.all()

        factory = request.GET.get('factory', None)
        name = request.GET.get('name', None)
        pandaq = request.GET.get('pandaq', None)
        
        if factory:
            labels = labels.filter(fid__name=factory)
        if name:
            jobs = jobs.filter(name=name)
        if pandaq:
            jobs = jobs.filter(pandaq__name=pandaq)

# limit
# offset
        fields = ('id','name','fid__name','msg','last_modified','queue', 'localqueue')

        data = list(labels.values(*fields))

        for d in data:
            jobs = Job.objects.filter(label=d['id'])
            d['ncreated'] = jobs.filter(state__name='CREATED').count()
            d['nrunning'] = jobs.filter(state__name='RUNNING').count()
            d['nexiting'] = jobs.filter(state__name='EXITING').count()
            d['ndone'] = jobs.filter(state__name='DONE').count()
            d['nfault'] = jobs.filter(state__name='FAULT').count()
            d['factory'] = d['fid__name']
            del d['fid__name']
            


        return HttpResponse(json.dumps(data,
                            cls=DjangoJSONEncoder,
                            sort_keys=True,
                            indent=2),
                            mimetype="application/json")

    context = 'HTTP method not supported: %s' % request.method
    return HttpResponse(context, status=405, mimetype="text/plain")

def factory(request, id):
    """
    Handle requests to /factories/{id} resource.

    GET:
    Return a factory and list of labels associated with the factory.

    PUT:
    Create or update factory arrtributes, intended to be called upon
    factory start-up. Accepts one or more of the following parameters:
        + name
        + email
        + url
        + version

    DELETE:
    Delete a factory, restricted to localhost
    """

    ip = request.META['REMOTE_ADDR']
    dt = datetime.now(pytz.utc) - timedelta(days=10)

    if request.method == 'GET':
        f = get_object_or_404(Factory, name=id)

        labels = Label.objects.filter(fid__name=id).values(
                               'name', 'msg', 'last_modified', 'pandaq__name')

        f = Factory.objects.filter(name=id).values('name', 'email',
                            'url', 'version', 'last_modified',
                            'last_startup')[0]
        f['labels'] = list(labels)

        return HttpResponse(json.dumps(f, 
                            cls=DjangoJSONEncoder,
                            sort_keys=True,
                            indent=2),
                            mimetype="application/json")

    if request.method == 'PUT':
        msg = "RAW REQUEST: %s %s %s" % (request.method, ip, request.body)
        logging.debug(msg)

        data = json.loads(request.body)

        defaults = {
                'email'   : data['email'],
                'url'     : data['url'],
                'version' : data['version'],
                'ip'      : ip,
                }

        if data.has_key('type'):
            defaults['factory_type'] = data['type']
                
        try:
            f, created = Factory.objects.get_or_create(name=id,
                                                       defaults=defaults)
        except:
            content = "Unable to create resource using: %s" % data
            return HttpResponseBadRequest(content, mimetype="text/plain")

        status = 201 if created else 200

        if f.factory_type != data['type']:
            f.factory_type = data['type']
            f.save()

        if f.email != data['email']:
            f.email = data['email']
            f.save() 

        if f.url != data['url']:
            f.url = data['url']
            f.save() 

        if f.version != data['version']:
            f.version = data['version']
            f.save() 

        if f.ip != ip:
            f.ip = ip
            f.save() 

        if created:
            mail_managers('Factory created: %s' % id,
                      'Factory created: %s' % id,
                      fail_silently=False)

        f.last_startup = datetime.now(pytz.utc)
        f.save()

        f = Factory.objects.filter(name=id).values()

        return HttpResponse(json.dumps(list(f), 
                            cls=DjangoJSONEncoder,
                            sort_keys=True,
                            indent=2),
                            status=status,
                            mimetype="application/json")

    if request.method == 'DELETE':
        if ip == '127.0.0.1':
            f = get_object_or_404(Factory, name=id)
            f.delete()
            return HttpResponse(mimetype="text/plain")
        else: 
            context = "Remote deletion is forbidden"
            return HttpResponseForbidden(context, mimetype="text/plain")

    context = 'HTTP method not supported: %s' % request.method
    return HttpResponse(context, mimetype="text/plain")

def factories(request):
    """
    Handle requests from /factories resource.

    GET:
    Return a list of all factories

    """

    ip = request.META['REMOTE_ADDR']

    if request.method != 'GET':
        context = 'HTTP method not supported: %s' % request.method
        return HttpResponse(context, mimetype="text/plain")

    dtactive = datetime.now(pytz.utc) - timedelta(days=10)

    factories = Factory.objects.values().order_by('name')
    for f in factories:
        active = True if f['last_modified'] > dtactive else False
        f['active'] = active

    return HttpResponse(json.dumps(list(factories), 
                        cls=DjangoJSONEncoder,
                        sort_keys=True,
                        indent=2),
                        mimetype="application/json")
