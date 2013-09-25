from django.core.management.base import BaseCommand, CommandError


class Command(BaseCommand):
    args = ''
    help = 'Imports data'

    def handle(self, *args, **options):
        pass
