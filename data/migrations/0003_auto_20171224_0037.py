# -*- coding: utf-8 -*-
# Generated by Django 1.11.8 on 2017-12-23 23:37
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('data', '0002_auto_20171224_0034'),
    ]

    operations = [
        migrations.AlterField(
            model_name='composition',
            name='mode',
            field=models.PositiveSmallIntegerField(blank=True),
        ),
        migrations.AlterField(
            model_name='composition',
            name='track_no',
            field=models.PositiveSmallIntegerField(blank=True),
        ),
    ]
