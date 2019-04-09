import os
import random
import unicodecsv
from django.conf import settings
from django.contrib.auth.models import User, Group

def create_default_groups():

    read = Group.objects.create(name='read')
    edit = Group.objects.create(name='edit')
    editplus = Group.objects.create(name='editplus')
    
    return (read, edit, editplus)

def create_anonymous_user():
    """
    Creates anonymous user and adds anonymous to appropriate groups.
    """

    anonymous_user = User.objects.create_user('anonymous', '', '')
    read_group = Group.objects.get(name='read')
    anonymous_user.groups.add(read_group)
    
def create_default_auth_system():

    default_groups = create_default_groups()
    create_anonymous_user()

    if not os.path.isfile(settings.INITIAL_USERS_CONFIG):
        return

    # remove the default Arches admin user (we want more control over this)
    admin_user = User.objects.get(username='admin')
    admin_user.delete()

    # now create users by loading the initial users CSV
    load_users(settings.INITIAL_USERS_CONFIG, overwrite=True)
    
def load_users(user_config_file, overwrite=False):

    # first get all groups and make sure they already exist in the system
    gdict = dict()
    with open(settings.INITIAL_USERS_CONFIG, "rb") as opencsv:
        reader = unicodecsv.DictReader(opencsv)
        for row, info in enumerate(reader):
            for g in info['groups'].split(";"):
                gname = g.lstrip().rstrip()
                try:
                    gobj = Group.objects.get(name=gname)
                except Group.DoesNotExist as e:
                    print "specified group '{}' in row {} does not exist. either "\
                        "create this group through the admin interface "\
                        "or modify the file and try again.".format(gname,row+1)
                    raise e
                gdict[gname] = gobj

    # now iterate the file again and create all the users
    with open(settings.INITIAL_USERS_CONFIG, "rb") as opencsv:
        reader = unicodecsv.DictReader(opencsv)
        print "\nCREATING USERS\n--------------"
        for info in reader:

            # check if user
            username = info["username"]
            if User.objects.filter(username=username).exists() and overwrite is False:
                print "  -- '{}' skipped: existing user will not be "\
                    "overwritten. Set overwrite = True if needed.".format(username)
                continue

            # create the user object from info in row
            # or overwrite existing user that matches the row
            user, created = User.objects.get_or_create(username=username)

            user.first_name=info['firstname']
            user.last_name=info['lastname']
            user.email=info['email']

            if info['staff'].lower().rstrip() == 'yes':
                user.is_staff = True
            if info['superuser'].lower().rstrip() == 'yes':
                user.is_superuser = True
            user.set_password(info['password'])
            user.save()

            # once saved, add the user to groups as needed
            for g in info['groups'].split(";"):
                gname = g.lstrip().rstrip()
                user.groups.add(gdict[gname])

            print "  --",user.username

def export_users(outfile):

    users = User.objects.all().exclude(username="anonymous")
    with open(outfile, "wb") as outcsv:
        writer = unicodecsv.writer(outcsv)
        writer.writerow(["username","firstname","lastname","email",
            "password","superuser","staff","groups"])

        for user in users:
            if user.username == "anonymous":
                continue
            if user.is_staff:
                staff = "yes"
            else:
                staff = ""
            if user.is_superuser:
                superuser = "yes"
            else:
                superuser = ""
            groups = ";".join([g.name for g in user.groups.all()])
            newpw = user.username.lower().replace(".","")+str(random.randint(1,1000)).zfill(3)
            row = [
                user.username,
                user.first_name,
                user.last_name,
                user.email,
                newpw,
                superuser,
                staff,
                groups
            ]
            writer.writerow(row)

    return outfile