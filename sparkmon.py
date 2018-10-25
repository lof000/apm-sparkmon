from lxml import html
import requests
import re
import json
import time

def gethtmlpage(urlPagina):
    page = requests.get(urlPagina)
    tree = html.fromstring(page.content)    
    return tree

def prepare(unformat_str):
    return unformat_str.strip().upper().replace(',','').replace('\n\r','').replace('\r\n','').replace('\n','').replace('\r','')

def prepareHostName(unformat_str):
    return unformat_str.strip().upper().replace('.','_').replace(':','_')

def prepareNumber(strnum):
    regex = '(\d+.\d+)(.*)'
    matches = re.search(regex, strnum )
    rn = matches.group(1).strip()
    un = matches.group(2).strip()
    val = 0
    if (un == 'B'):
        val = float(rn)*1024*1024
    if (un == 'MB'):
        val = float(rn)*1024
    if (un == 'GB'):
        val = float(rn)
    return int(val)

def translateStatus(status):
    if status == 'ALIVE':
        return 1
    return 0

def getmastertitle(st):
    tit = prepare(''.join(st))
    regex = 'SPARK MASTER AT SPARK:\/\/(.*)'
    matches = re.search(regex, tit )
    title = matches.group(1).strip()
    return title

def tratacores(st):
    regex = '(.*)TOTAL(.*)USED'
    matches = re.search(regex, st  )
    total = matches.group(1).strip()
    used =  matches.group(2).strip()
    return total,used

def tratadrivers(st):
    regex = '(.*)RUNNING(.*)COMPLETED'
    matches = re.search(regex, st  )
    rn = matches.group(1).strip()
    cp =  matches.group(2).strip()
    return rn,cp

def trataapps(st):
    regex = '([0-9]+).+([0-9]+)'
    matches = re.search(regex, st  )
    rn = matches.group(1).strip()
    cp =  matches.group(2).strip()
    return rn,cp

def trataworkercores(st):
    regex = '(.*)\((.*)USED\)'
    matches = re.search(regex, st  )
    total = matches.group(1).strip()
    used =  matches.group(2).strip()
    return total,used

def trataworkermemory(st):
    regex = '(.*)\((.*)USED\)'
    matches = re.search(regex, st  )
    total = matches.group(1).strip()
    used =  matches.group(2).strip()
    return total,used


def getmasterdata(page):
    #page = gethtmlpage(masterurl)
    itens = page.xpath('//div[@class="span12"][1]/ul/li')
    metrics_raw = {}
    #for item in itens[1:]:
    for item in itens:    
        x = item.xpath('strong/text()')
        y = item.xpath('text()')
        t = prepare(''.join(y))
        m = x[0]
        if m == "REST URL:":
            metrics_raw[m] = t
        if m == "Alive Workers:":
            metrics_raw[m] = t
        if m == "Cores in use:":
            total,used = tratacores(t)
            metrics_raw['CORES_TOTAL'] = total
            metrics_raw['CORES_USED'] = used
        if m == "Memory in use:":
            total,used = tratacores(t)
            metrics_raw['MEMORY_TOTAL'] = total
            metrics_raw['MEMORY_USED'] = used
        if m == "Applications:":
            r,c=trataapps(t)
            metrics_raw['RUNNING_APPS'] = r
            metrics_raw['COMPLETED_APPS'] = c
        if m == "Drivers:":
            r,c = tratadrivers(t)
            metrics_raw['RUNNING_DRV'] = r
            metrics_raw['COMPLETED_DRV'] = c
        if m == "Status:":
            metrics_raw[m] = t
    t = page.xpath('//h3[@style="vertical-align: middle; display: inline-block;"]/text()')
    title = getmastertitle(t)
    metrics_raw['MASTER_HOST'] = title
    return metrics_raw

def getworkermaindata(page):
    #page = gethtmlpage(masterurl)
    itens = page.xpath('//div[@class="span12"][1]/ul/li')
    worker_metrics_raw = {}
    #for item in itens[1:]:
    for item in itens:    
        x = item.xpath('strong/text()')
        y = item.xpath('text()')
        t = prepare(''.join(y))
        m = x[0].strip()
        if m == "Cores:":
            mem_tot,mem_used = trataworkercores(t)
            worker_metrics_raw['CORES_TOTAL'] = mem_tot
            worker_metrics_raw['CORES_USED'] = mem_used
        if m == "Memory:":
            mem_tot,mem_used = trataworkermemory(t)
            worker_metrics_raw['MEMORY_TOTAL'] = mem_tot
            worker_metrics_raw['MEMORY_USED'] = mem_used
        if m == "ID:":
            worker_metrics_raw['WORKER_ID'] = t
    return worker_metrics_raw

def getworkerslink(page):
    #itens = page.xpath('//tr/td/a')
    itens = page.xpath('/html/body/div/div[3]/div/table/tbody/tr/td/a')

    workers_links = []
    for item in itens:
        workers_links.append(item.get('href'))   
    return workers_links
        

def getdatafromspark(url):
    page = gethtmlpage(url)
    metrics_raw = getmasterdata(page)
    workers = []
    workers_links = getworkerslink(page)
    for link in workers_links:
        p =  gethtmlpage(link)
        worker_data = getworkermaindata(p)
        workers.append(worker_data)
    metrics_raw['workers'] = workers
    return metrics_raw

def getmastermetricname(masterhost,compname,name):
    #return "Spark|"+masterhost+"|"+compname+":"+name
    return "Spark|"+masterhost+":"+name

def getworkermetricname(masterhost,worker,compname,name):
    return "Spark|"+masterhost+"|"+ worker+":"+name    

def makemetric(type,masterhost,name,value):
    metric = {"type" : type, "name" : getmastermetricname(masterhost,'compname',name), "value" : value}
    return metric

def makeworkermetric(type,masterhost,workername,name,value):
    metric = {"type" : type, "name" : getworkermetricname(masterhost,workername,'compname',name), "value" : value}
    return metric

def getApmMetrics(spark_data):
    apm_metrics = {'metrics': ''}

    #master data
    master_alivew = makemetric('IntCounter',prepareHostName(spark_data['MASTER_HOST']),'Alive Workers',spark_data['Alive Workers:'])  
    master_cputotal = makemetric('IntCounter',prepareHostName(spark_data['MASTER_HOST']),'Cores Total',spark_data['CORES_TOTAL'])  
    master_cpuused = makemetric('IntCounter',prepareHostName(spark_data['MASTER_HOST']),'Cores Used',spark_data['CORES_USED'])
    master_memtotal = makemetric('IntCounter',prepareHostName(spark_data['MASTER_HOST']),'Memory Total',prepareNumber(spark_data['MEMORY_TOTAL']))
    master_memused = makemetric('IntCounter',prepareHostName(spark_data['MASTER_HOST']),'Memory Used',prepareNumber(spark_data['MEMORY_USED']))
    master_run_apps = makemetric('IntCounter',prepareHostName(spark_data['MASTER_HOST']),'Running Applications',spark_data['RUNNING_APPS'])
    master_run_drvs = makemetric('IntCounter',prepareHostName(spark_data['MASTER_HOST']),'Running Drivers',spark_data['RUNNING_DRV'])
    status = makemetric('IntCounter',prepareHostName(spark_data['MASTER_HOST']),'Status',translateStatus(spark_data['Status:']))
    m = [master_alivew,master_cputotal,master_cpuused,master_memtotal,master_memused,master_run_apps,master_run_drvs,status]

    #worker data
    workers_data = spark_data['workers']
    worker_metrics = []
    for wd in workers_data:
        m.append(makeworkermetric('IntCounter',prepareHostName(spark_data['MASTER_HOST']),wd['WORKER_ID'],'Memory Total',prepareNumber(spark_data['MEMORY_TOTAL'])))
        m.append(makeworkermetric('IntCounter',prepareHostName(spark_data['MASTER_HOST']),wd['WORKER_ID'],'Memory Used',prepareNumber(spark_data['MEMORY_USED'])))

    apm_metrics['metrics'] = m
    return apm_metrics

def sendToApm(apm_metrics):
    strmetrics = json.dumps(apm_metrics)
    headers = {
        'Content-Type': 'application/json',
    }
    r = requests.post(url = APM_EPA_URL, headers=headers, data = strmetrics) 
    #print(r.status_code)
    return

def getConfig():
    with open('config.json', 'r') as f:
        config = json.load(f)
    return config


def collectData():
    spark_data_old = {}
    spark_data_final = {}
    print("-------------------------------------------")
    print("Collecting data from SPARK")
    print("Spark    :",SPARK_URL)
    print("APM_EPA  :",APM_EPA_URL)
    print("POLLING(S)  :",POLLING)
    print("-------------------------------------------")
    while True:
        try:
            spark_data = getdatafromspark(SPARK_URL)
            apm_metrics = getApmMetrics(spark_data)
            sendToApm(apm_metrics)
            time.sleep(POLLING)
        except Exception as e:
            print('Erro !', e)
            time.sleep(POLLING)
            break


configfile = getConfig()
SPARK_URL = configfile["spark_console_url"]
APM_EPA_URL = 'http://'+configfile["apm_epa_host"]+':'+configfile["apm_epa_port"]+'/apm/metricFeed'
POLLING = int(configfile["polling_seconds"])

collectData()


