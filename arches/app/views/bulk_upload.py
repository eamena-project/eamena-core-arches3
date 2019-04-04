'''
ARCHES - a program developed to inventory and manage immovable cultural heritage.
Copyright (C) 2013 J. Paul Getty Trust and World Monuments Fund

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
'''

import os
import json
import time
from StringIO import StringIO
from django import forms
from django.conf import settings
from django.core.management import call_command
from django.contrib.auth.decorators import login_required, user_passes_test
from django.template import RequestContext
from django.shortcuts import render_to_response
from django.http import HttpResponse
from arches.app.models.resource import Resource
from arches.app.utils.imageutils import generate_thumbnail

def handle_uploaded_file(f):
    '''the actual file upload happens here, and returns a path to the file
    where it exists on the server'''
    dest_path = os.path.join(settings.BULK_UPLOAD_DIR,str(f))
    with open(dest_path, 'wb+') as destination:
        for chunk in f.chunks():
            destination.write(chunk)
    return dest_path

def get_archesfile_path(filepath):
    '''takes the input spreadsheet path and outputs a path for a new .arches
    file inside of the BULK_UPLOAD_DIR.'''

    basename = os.path.splitext(os.path.basename(filepath))[0].replace(" ","_")
    name = time.strftime("{}_%H%M%d%m%Y.arches".format(basename))
    destpath = os.path.join(settings.BULK_UPLOAD_DIR,name)

    return destpath

@login_required
@user_passes_test(lambda u: u.groups.filter(name='edit').count() != 0, login_url='/auth/')
def new_upload(request):
    ''' nothing special here, everything is handled with ajax'''

    return render_to_response('bulk-upload/new.htm',
        {'active_page': 'Bulk Upload'}, # not sure if this is necessary
        context_instance=RequestContext(request) # or this
    )

@login_required
@user_passes_test(lambda u: u.groups.filter(name='edit').count() != 0, login_url='/auth/')
def main(request):
    ''' nothing special here, everything is handled with ajax'''

    loadlog = settings.BULK_UPLOAD_LOG_FILE
    if os.path.isfile(loadlog):
        with open(loadlog,'rb') as loadlog:
            loads = loadlog.readlines()
    else:
        loads = []
    return render_to_response('bulk-upload/main.htm',
        {'active_page': 'Bulk Upload',
        'load_log':loads}, # not sure if this is necessary
        context_instance=RequestContext(request) # or this
    )

def validate(request):
    '''this view is designed to be hit with an ajax call that includes the path
    to the spreadsheet on the server, and the resource type for the spreadsheet
    '''

    fpath = request.POST.get('filepath', '')
    restype = request.POST.get('restype','')
    valtype = request.POST.get('validationtype','')

    convert = {'false':False,'true':True}
    append = convert[request.POST.get('append', 'false')]

    fullpath = os.path.join(settings.BULK_UPLOAD_DIR,str(fpath))
    destpath = get_archesfile_path(fullpath)
    out = StringIO()
    call_command('Excelreader',
        operation='validate',
        source=fullpath,
        dest_dir=destpath,
        res_type=restype,
        append_data=append,
        validation_type=valtype,
        stdout=out,
    )

    errorlog = os.path.join(settings.BULK_UPLOAD_DIR,
        "{}_validation_errors-{}.json".format(os.path.basename(os.path.splitext(destpath)[0]),valtype)
    )
    with open(errorlog,'r') as readjson:
        data = readjson.read()
        result = json.loads(data)

    if restype == 'relations':
        destpath = destpath.replace('.arches', '.relations')

    if not result['success']:
        os.remove(fullpath)

    else:
        result['filepath'] = os.path.basename(destpath)

    if "Has attachments" in out.getvalue():
        result['hasfiles'] = True

    return HttpResponse(json.dumps(result), content_type="application/json")
    
def upload_spreadsheet(request):
    '''this is the view that handles the file upload ajax call. it includes a
    very simple test for the file format, which should be XLSX, and returns the
    file path, name, and validity, all of which are used on the front-end.'''
    
    if request.method == 'POST':
        f = request._files['files[]']
        fname = os.path.basename(str(f))
        response_data = {
            'filevalid':True,
            'filename':fname,
            'filepath':'',
        }
        ## simple test for file type; don't upload non-excel files
        if not fname.endswith('.xlsx'):
            response_data['filevalid'] = False
        else:
            fpath = handle_uploaded_file(f)
            response_data['filepath'] = os.path.basename(fpath)

        return HttpResponse(json.dumps(response_data), content_type="application/json")

def import_archesfile(request):
    '''just a wrapper to the load_resources command. expects the name of a file
    that is in the BULK_UPLOAD_DIR directory. returns nothing because the load
    results are written to a log file.'''

    fpath = request.POST.get('filepath','')
    fullpath = os.path.join(settings.BULK_UPLOAD_DIR,fpath)
    append = request.POST.get('append', 'false')
    restype = request.POST.get('restype', '')

    output = StringIO()
    try:
        if restype == 'relations':
            call_command('packages',
                         operation='load_relations',
                         source=fullpath,
                         appending=append,
                         run_internal=True,
                         stdout=output,
                         )
        else:
            call_command('packages',
                         operation='load_resources',
                         source=fullpath,
                         appending=append,
                         run_internal=True,
                         stdout=output,
                         )
    except Exception as e:
        print e

    val = output.getvalue().strip()
    return HttpResponse(json.dumps(val), content_type="application/json")

def upload_attachments(request):
    """
    We'll enter this view once for the uploaded folder. For each file we need to find which resource it
    belongs to (if any) and add that entry. We're pulling a dictionary of old and new resource ids out from the load_resources
    process and using that to update and edit the file entities.
    """

    response_data = {
        'success': True,
        'errors': []
    }

    if request.method == 'POST':
        resdict = json.loads(request.POST['resdict'])
        myfiles = {}
        for f in request.FILES.getlist('attachments[]'):
            filename, ext = os.path.splitext(os.path.basename(str(f)))
            if ext == '.xlsx':
                continue
            myfiles[f._name.replace(" ", "_")] = f

        num_updated = 0
        archesfile = request.POST['archesfile']
        archesfilepath = os.path.join(settings.BULK_UPLOAD_DIR, archesfile)
        with open(archesfilepath, 'r') as ins:
            for l in ins:
                if 'FILE_PATH' in l:
                    data = l.split('|')
                    if data[3] in myfiles.keys():
                        f = myfiles[data[3]]
                        if data[0] not in resdict:
                            response_data['success'] = False
                            response_data['errors'].append('Unable to find resource ID information for %s. Did you load the resources?' % data[0])
                        resid = resdict[data[0]]
                        res = Resource(resid)
                        res.set_entity_value('FILE_PATH.E62', f)
                        thumb = generate_thumbnail(f)
                        if thumb != None:
                            res.set_entity_value('THUMBNAIL.E62', thumb)
                        res.save()
                        num_updated += 1

        if num_updated != len(resdict.keys()):
            response_data['success'] = False
            response_data['errors'].append('Not all files could be found in the uploaded directory.')

    return HttpResponse(json.dumps(response_data), content_type="application/json")


def undo_load(request):
    """
    This will remove the loaded resources from the database.
    """

    response_data = {
        'success': True,
        'errors': [],
    }

    if request.method == 'POST':
        load_id = request.POST.get('load_id')
        if load_id:
            call_command('packages',
                         operation='remove_resources',
                         load_id=load_id,
                         force=True
                         )
        else:
            response_data['success'] = False
            response_data['errors'].append('Load ID was not found.')

    return HttpResponse(json.dumps(response_data), content_type="application/json")
