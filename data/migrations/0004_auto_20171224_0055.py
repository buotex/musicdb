# -*- coding: utf-8 -*-
# Generated by Django 1.11.8 on 2017-12-23 23:55
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('data', '0003_auto_20171224_0037'),
    ]

    operations = [
        migrations.AlterField(
            model_name='composition',
            name='mode',
            field=models.SmallIntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='composition',
            name='track_no',
            field=models.SmallIntegerField(blank=True, null=True),
        ),
    ]
