from django.db import models

"""
Model to represent the ATLAS topology of Sites and resources
"""

CLOUDS = (
        ('CA', 'CA'),
        ('CERN', 'CERN'),
        ('DE', 'DE'),
        ('ES', 'ES'),
        ('FR', 'FR'),
        ('IT', 'IT'),
        ('ND', 'ND'),
        ('NL', 'NL'),
        ('TW', 'TW'),
        ('UK', 'UK'),
        ('US', 'US'),
        )

QTYPE = (
        ('ANALYSIS_QUEUE', 'ANALYSIS_QUEUE'),
        ('PRODUCTION_QUEUE', 'PRODUCTION_QUEUE'),
        ('SPECIAL_QUEUE', 'SPECIAL_QUEUE'),
        )


class Cloud(models.Model):
    """
    Represents an ATLAS cloud
    """
    name = models.CharField(max_length=8, choices=CLOUDS, blank=True, unique=True)
    def __unicode__(self):
        return self.name
    class Meta:
        ordering = ['name']

class Tag(models.Model):
    """
    tag string, used to select subset of sites
    """
    name = models.CharField(max_length=40)
    def __unicode__(self):
        return self.name

class Site(models.Model):
    """
    Represents a GOCDB/SSB/AGIS site, single sysadmin control
    """
    name = models.CharField(max_length=128, unique=True)
    gocdbname = models.CharField(max_length=128, unique=True, null=True)
    ssbname = models.CharField(max_length=128, null=True)
    pandasitename = models.CharField(max_length=128, null=True)
    cloud = models.ForeignKey(Cloud, blank=True, null=True)
    tags = models.ManyToManyField(Tag, blank=True)
    last_modified = models.DateTimeField(auto_now=True, editable=False, null=True)
    def __unicode__(self):
        return self.name
    class Meta:
        ordering = ['name']

class PandaSite(models.Model):
    """
    Represents a Panda Site (siteid), first column on 'clouds' page
    """
    name = models.CharField(max_length=512)
    site = models.ForeignKey(Site, blank=True, null=True)
    tier = models.CharField(max_length=8, blank=True, null=True)
    def __unicode__(self):
        return self.name
    class Meta:
        ordering = ['name']

class PandaQueue(models.Model):
    """
    Represents a Panda queue (nickname)
    """
    name = models.CharField(max_length=512, unique=True)
    pandasite = models.ForeignKey(PandaSite, blank=True, null=True)
    state = models.CharField(max_length=16, blank=True, default='unknown')
    tags = models.ManyToManyField(Tag, blank=True)
    comment = models.CharField(max_length=140, blank=True, default='')
    type = models.CharField(max_length=32, choices=QTYPE, blank=True, null=True)
    control = models.CharField(max_length=32, blank=True, null=True)
    # this should be upstream timestamp. ie. from SSB json data
    timestamp = models.DateTimeField(editable=False, null=True)
    last_modified = models.DateTimeField(auto_now=True, editable=False, null=True)
    def __unicode__(self):
        return self.name
    class Meta:
        ordering = ['name']
