from optparse import make_option
import textwrap

from django.core.management.base import BaseCommand
from django.db import connections, transaction
from django.conf import settings

from pthelma import timeseries
from enhydris.hcore import models

from ._hcheck import ExternalDataChecker


def commit_all_databases():
    for db in settings.DATABASES:
        transaction.commit(using=db)


def rollback_all_databases():
    for db in settings.DATABASES:
        transaction.rollback(using=db)


class Command(BaseCommand):
    option_list = BaseCommand.option_list + (
        make_option('--dry-run',
                    action='store_true',
                    dest='dry_run',
                    default=False,
                    help='Rollback all changes when finished'),
    )
    help = textwrap.dedent('''\
        Import data files into Enhydris.

        The current directory must be a directory with files as specified in
        http://hydroscope.gr/, Documents, Data entry standardization. This
        command checks them and inserts them into the databases as needed.''')

    def handle(self, *args, **options):
        c = ExternalDataChecker()
        c.check()
        try:
            errors = False
            for h in c.hts_entries:
                try:
                    self.process_file(h['filename'], h['station_id'],
                                      h['variable_id'], h['step_id'])
                except Exception as e:
                    print('{0}: {1}'.format(h['filename'], str(e)))
                    errors = True
        except:
            rollback_all_databases()
            raise
        if options['dry_run'] or errors:
            print "Rolling back"
            rollback_all_databases()
        else:
            print "Committing"
            commit_all_databases()
    for db in settings.DATABASES:
        handle = transaction.commit_manually(using=db)(handle)

    def process_file(self, filename, station_id, variable_id, step_id):
        gentity = models.Gentity.objects.using('default').get(pk=station_id)
        db = gentity.original_db.hostname.split('.')[0]
        station_local_id = gentity.original_id
        ts, created = models.Timeseries.objects.using(db).get_or_create(
            gentity__id=station_local_id, variable__id=variable_id,
            time_step__id=step_id)
        t = timeseries.Timeseries(ts.id)
        t.read_from_db(connections[db])
        nexisting_records = len(t)
        t1 = timeseries.Timeseries()
        with open(filename) as f:
            t1.read_file(f)
        t.append(t1)
        t.write_to_db(connections[db], commit=False)
        print(
            'Station {0}, {1}, {2}, {3} timeseries, {4} + {5} records'
            .format(station_id, db, station_local_id,
                    'new' if created else 'existing',
                    nexisting_records, len(t1)))
