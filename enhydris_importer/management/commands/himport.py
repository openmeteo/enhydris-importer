import logging

from django.core.management.base import BaseCommand
from django.db import connections, transaction
from django.conf import settings

import pthelma
from enhydris.hcore import models

from .hcheck import ExternalDataChecker


logger = logging.getLogger(__name__)


def commit_all_databases():
    for db in settings.DATABASES:
        transaction.commit(using=db)


def rollback_all_databases():
    for db in settings.DATABASES:
        transaction.rollback(using=db)


class Command(BaseCommand):
    args = ''
    help = 'Imports data'

    @transaction.commit_manually
    def handle(self, *args, **options):
        c = ExternalDataChecker()
        c.check()
        try:
            for h in c.hts_entries:
                self.process_file(h['filename'], h['station_id'],
                                  h['variable_id'], h['step_id'])
        finally:
            # Temporary until we make sure it works
            rollback_all_databases()

    def process_file(filename, station_id, variable_id, step_id):
        gentity = models.Gentity.objects.using('main').get(pk=station_id)
        db = gentity.original_db.hostname.split('.')[0]
        station_local_id = gentity.original_id
        ts, created = models.Timeseries.objects.using(db).get_or_create(
            gentity__id=station_local_id, variable__id=variable_id,
            time_step__id=step_id)
        t = pthelma.Timeseries(ts.id)
        t.read_from_db(connections[db])
        nexisting_records = len(t)
        t1 = pthelma.Timeseries()
        with open(filename) as f:
            t1.read_file(f)
        t.append(t1)
        t.write_to_db(connections[db], commit=False)
        logger.debug(
            'Station {0}, {1}, {2}, {3} timeseries, {4} + {5} records'
            .format(station_id, db, station_local_id,
                    'new' if created else 'existing',
                    nexisting_records, len(t1)))
