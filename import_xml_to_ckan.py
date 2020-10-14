# -*- coding: utf-8 -*-
import urllib2
import json
import time
import re
import sys
import glob

import import_xml_to_ckan_util as importlib


# TODO: known issue 'doelstelling' is too large for solr.
# TODO: Solr limit is 32766 bytes, have to replace solr string field with text field
def create_package(org, f, apikey):
    pattern = re.compile('[^a-zA-Z0-9_-]+')
    dataset_dict = dict()

    dataset_dict[u'owner_org'] = org
    dataset_dict[u'extras'] = list()

    with open(f, 'r') as fh:
        # load json from file obj
        data = json.load(fh)
        # get vereniging info
        vereniging = data.get('vereniging', None)

        # loop through vereniging and add k, v to data_dict
        for k, v in vereniging.items():
            if (k == u'naam'):
                # print('key: {}; value: {}'.format(k, v))
                dataset_dict[u'title'] = v
                name = pattern.sub('-', v.lower())
                # check if the name is being used already
                response_dict = importlib.get_package_by_name(name, apikey=apikey)
                if response_dict:
                    # the name is used, append city name to it
                    city = vereniging.get('plaats', None) if vereniging.get('plaats', None) else vereniging.get(
                        'gemeente', 'unknown_gemeente')
                    city = pattern.sub('-', city.lower())
                    name = '{}-{}'.format(name, city)
                dataset_dict[u'name'] = name
            elif (k == u'opmerkingen'):
                # print('key: {}; value: {}'.format(k, v))
                dataset_dict[u'notes'] = v
            else:
                dataset_dict[u'extras'].append({u'key': k, u'value': json.dumps(v)})

    # Use the json module to dump the dictionary to a string for posting
    data_string = urllib2.quote(json.dumps(dataset_dict))

    # use the package_create function to create a new dataset
    request = urllib2.Request(
        'http://localhost:5000/api/3/action/package_create')

    # add authorization header
    request.add_header('Authorization', apikey)

    # Make the HTTP request.
    response = urllib2.urlopen(request, data_string)
    assert response.code == 200, dataset_dict

    # Use the json module to load CKAN's response into a dictionary.
    response_dict = json.loads(response.read())
    assert response_dict.get('success', False) is True

    # package_create returns the created package as its result.
    # created_package = response_dict['result']
    # pprint(created_package)
    return True


def __main__():
    start = time.time()

    print('start')
    # init the config
    importlib.init()

    args = None
    try:
        args = sys.argv[1]
    except IndexError:
        exit('organization is required on the command line')

    try:
        clean = sys.argv[2]
    except IndexError:
        clean = False

    if args in ('spd', 'sdb'):
        org = 'sdb'
    else:
        raise Exception('Invalid organization')

    wd = importlib.orgs[org][4]
    apikey = importlib.apikey
    debug = importlib.debug
    qty = importlib.qty

    # set page style
    if importlib.set_title_homepage_style():
        print('#### Website title and home page style set successfully')
    else:
        print('#### Website title and home page style set failed')

    # if not org Create it
    if not importlib.org_exists(org):
        print('organization [%s] does not exist. Creating!' % org)
        if importlib.create_org(org):
            print('organization [%s] created!' % org)
        else:
            exit('organization [%s] cannot be created!' % org)
    else:
        print('organization [%s] already exists.' % org)

    created_package = importlib.get_created_package(org, apikey)
    print('From outside loop: %s created packages; debug is: %s; clean is: %s' % (len(created_package), debug, clean))
    while len(created_package) > 0 and (debug or clean):
        print('From inside loop: %s created packages; debug is: %s; clean is: %s' % (
            len(created_package), debug, clean))
        created_package = importlib.get_created_package(org, apikey)
        # created_package = get_all_created_package(apikey)
        importlib.remove_all_created_package(created_package, apikey)
        if clean:
            print('cleaning old datasets')
        else:
            print('removing old dataset')
    else:
        print('removed dataset')

    # files = [join(wd, f) for f in sorted(listdir(wd)) if f.endswith('.json') and isfile(join(wd, f))]
    files = glob.glob("{}/*.json".format(wd)) + glob.glob("{}/**/*.json".format(wd))
    print('get file lists')
    counter = 0

    for f in files:
        print('### start with file: %s ###' % f)
        result = create_package(org, f, apikey=apikey)

        if counter > qty - 1 and debug:
            break
        if result:
            counter += 1
        print('### end with file: %s ###' % f)
    end = time.time()
    elapsed = end - start
    print('#### Overview ###')
    print('#### Start at: %s' % start)
    print('#### Ends at: %s' % end)
    print('#### Time elapsed: %s' % elapsed)


__main__()
