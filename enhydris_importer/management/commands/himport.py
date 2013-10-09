from optparse import make_option
import textwrap
from datetime import datetime

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
        make_option('--only-metadata',
                    action='store_true',
                    dest='only_metadata',
                    default=False,
                    help='Only enter the metadata; assume the data is already '
                         'entered'),
        make_option('--comment',
                    action='store',
                    dest='comment',
                    default='Data from {0:%Y-%m-%d %H:%M} to '
                            '{1:%Y-%m-%d %H:%M} entered with himport on '
                            '{2:%Y-%m-%d %H:%M}',
                    help='Append specified comment to remarks field for all '
                         'time series; if the comment contains {0}, {1} or '
                         '{2}, they are replaced with the start and end dates '
                         'of the time series and the current date; these '
                         'can optionally include format specifiers, like '
                         '{0:%Y-%m-%d %H:%M}. The default is '
                         '"Data from {0:%Y-%m-%d %H:%M} to '
                         '{1:%Y-%m-%d %H:%M} entered with himport on '
                         '{2:%Y-%m-%d %H:%M}"'),
        make_option('--ignore-spreadsheet',
                    action='store_true',
                    dest='ignore_spreadsheet',
                    default=False,
                    help="Don't cross-check data with the spreadsheet"),
    )
    help = textwrap.dedent('''\
        Import data files into Enhydris.

        The current directory must be a directory with files as specified in
        http://hydroscope.gr/, Documents, Data entry standardization. This
        command checks them and inserts them into the databases as needed.''')

    def handle(self, *args, **options):
        self.options = options
        c = ExternalDataChecker()
        if not options['ignore_spreadsheet']:
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
        local_gentity = models.Gentity.objects.using(db).get(
            pk=gentity.original_id)
        local_variable = models.Variable.objects.using(db).get(pk=variable_id)
        local_step = models.TimeStep.objects.using(db).get(pk=step_id)
        try:
            ts = models.Timeseries.objects.using(db).get(
                gentity=local_gentity, variable=local_variable,
                time_step=local_step)
            created = False
        except models.Timeseries.DoesNotExist:
            unit_of_measurement = models.UnitOfMeasurement.objects.using(
                db).get(variables__exact=local_variable)
            time_zone = models.TimeZone.objects.using(db).get(code='EET')
            ts = models.Timeseries(
                gentity=local_gentity,
                variable=local_variable, time_step=local_step,
                actual_offset_minutes=0, actual_offset_months=0,
                unit_of_measurement=unit_of_measurement, time_zone=time_zone)
            ts.save(using=db)
            created = True
        t = timeseries.Timeseries(ts.id)
        t.read_from_db(connections[db])
        nexisting_records = len(t)
        t1 = timeseries.Timeseries()
        with open(filename) as f:
            t1.read_file(f)
        if not self.options['only_metadata']:
            t.append(t1)
            t.write_to_db(connections[db], commit=False)
        self.add_comment(ts, t1.bounding_dates())
        print(
            'Station {0}, {1}, {2}, {3} timeseries, {4} + {5} records'
            .format(station_id, db, local_gentity.id,
                    'new' if created else 'existing',
                    nexisting_records, len(t1)))

    def add_comment(self, ts, bounding_dates):
        remarks = ts.remarks.strip()
        separator = '\n\n' if ts.remarks else ''
        ts.remarks = remarks + separator + self.options['comment'].format(
            bounding_dates[0], bounding_dates[1], datetime.now())
        ts.save()
