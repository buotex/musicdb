from django.db import models

# Create your models here.
class Composer(models.Model):
    name = models.TextField()

class Composition(models.Model):
    track_no = models.SmallIntegerField(null=True, blank=True)
    title = models.TextField()
    remark = models.TextField()
    mode = models.SmallIntegerField(null=True, blank=True)
    printed_in = models.TextField()
    genre = models.TextField()
    scribe = models.TextField()
