import os
import oursql
import json
import urllib2
import itertools

from collections import defaultdict
from urllib import urlencode

DB_CONFIG_PATH = os.path.expanduser('~/replica.my.cnf')
MW_API_URL = 'https://{lang}.{project}.org/w/api.php?'

def grouper(iterable, n):
    '''\
    http://stackoverflow.com/questions/3992735/
        python-generator-that-groups-another-iterable-into-groups-of-n
    '''
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, n))
        if not chunk:
            return
        yield chunk


def get_titles(page_ids):
    lang = 'en'
    project = 'wikipedia'
    params = {'action': 'query',
              'prop': 'info',
              'pageids': '|'.join([str(p) for p in page_ids]),
              'format': 'json'}
    url = MW_API_URL.format(lang=lang, project=project)
    resp = urllib2.urlopen(url + urlencode(params))
    data = json.loads(resp.read())
    ret = {}
    for page_id, page_info in data['query']['pages'].iteritems():
        if int(page_id) < 0:
            ret[page_id] = None
        else:
            try:
                ret[page_id] = (page_info['title'], page_info['ns'])
            except Exception as e:
                ret[page_id] = None
    return ret


def save(data, year, limit):
    out_path = '%s-%s.json' % (year, limit)
    try:
        out_file = open(out_path, 'w')
    except Exception as e:
        import pdb; pdb.set_trace()
    with out_file:
        json.dump(data, out_file)
        
  
def load(file_name):
    try:
        in_file = open(file_name, 'r')
    except Exception as e:
        import pdb; pdb.set_trace()
    with in_file:
        data = json.load(in_file)
    yearly = defaultdict(int)
    for month in data:
        for article in month:
            pid = article['page']
            edits = article['edits']
            yearly[pid] += edits
    ret = []
    groups = grouper(yearly, 50)
    for group in groups:
        titles = get_titles(group)
        for pid in group:
            ret.append({'id': pid,
                        'edits': yearly[pid],
                        'title': titles[str(pid)][0],
                        'ns': titles[str(pid)][1]})
    ret = sorted(ret, key=lambda p: p['edits'], reverse=True)
    top = [r for r in ret if r['ns'] == 0][:10]
    return top
  def get_most_edited(year=2015, month=01, lang='en', limit=1000):
    query = '''
    SELECT
    LEFT(rev_timestamp, 6) AS month,
         rev_page AS page,
         COUNT(*) AS edits
    FROM revision
    WHERE rev_timestamp BETWEEN ? AND ?
    GROUP BY month, page
    ORDER BY month ASC, edits DESC
    LIMIT ?;'''

    db_title = lang + 'wiki_p'
    db_host = lang + 'wiki.labsdb'
    connection = oursql.connect(db=db_title,
                                host=db_host,
                                read_default_file=DB_CONFIG_PATH,
                                charset=None)
    cursor = connection.cursor(oursql.DictCursor)
    params = ['%02d%02d' % (year, month),
              '%02d%02d' % (year, month + 1),
              limit]
    cursor.execute(query, params)
    results = cursor.fetchall()
    return results


def get_year(year):
    results = []
    for month in range(1, 13):
        print 'getting data for %s' % month
        data = get_most_edited(year=year, month=month)
        results.append(data)
    return results


if __name__ == '__main__':
    #
    # TODO: Clean up
    #
    #for year in range (2001, 2016):
    #    print 'Getting %s' % year
    #    print '======='
    #    res = get_year(year)
    #    save(res, year, 1000)
    for year in range(2001, 2016):
        file_name = '%s-1000.json' % year
        top = load(file_name)
        print '==='
        print year
        print '---'
        print top
    import pdb; pdb.set_trace()
    