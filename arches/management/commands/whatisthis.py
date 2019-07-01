from optparse import make_option
from django.core import management
from django.core.management.base import BaseCommand, CommandError
from django.db.models import get_app, get_models
from django.db.utils import ProgrammingError
import uuid

class Command(BaseCommand):

    help = 'finds any arches objects whose primary key is the input uuid and'\
        'returns these objects in the form of a list.'

    option_list = BaseCommand.option_list + (
        make_option('-u', '--uuid', action='store', default='',
            help='uuid to find'
        ),
    )

    def handle(self, *args, **options):
        self.find_uuid(options['uuid'])

    def find_uuid(self,in_uuid):
    
        ## check for valid uuid input
        val = uuid.UUID(in_uuid, version=4)
        try:
            val = uuid.UUID(in_uuid, version=4)
        except ValueError:
            print("  -- this is not a valid uuid")
            return False

        ## search all models and see if the UUID matches an existing object
        objs = []
        app = get_app('models')
        for m in get_models(app):
        # for m in django.apps.apps.get_models():
            
            # if not m.__module__.startswith('arches'):
                # continue
            # print m._meta.pk.get_internal_type() #!= "UUIDField":
                # continue
            # print m
            if m.__doc__.startswith("Class") or m.__doc__.startswith("VwNodes"):
                 continue
            try:
                ob = m.objects.filter(pk=in_uuid)
            except:
                continue
            # print ob
            objs+=ob

        ## return False if nothing was found
        if not objs:
            print("  -- this uuid doesn't match any objects in your database")
            return False
        
        ## print summary of found objects
        print(80*"=")
        print("This UUID is the primary key for {} object{}:".format(
            len(objs),"s" if len(objs) > 1 else ""))
        for o in objs:
            print(80*"-")
            print(o)
            keys = vars(o).keys()
            keys.sort()
            for k in keys:
                print(k)
                print("  {}".format(vars(o)[k]))
        print(80*"=")
        return objs