from __future__ import print_function 
from flask import abort
import pypyodbc
from flask import Flask, jsonify

app = Flask(__name__)
cnxn =''


def getQdetails  ():
    cursor = cnxn.cursor()
    cursor.execute("SELECT csqname FROM RtCSQsSummary")
    rows = cursor.fetchall()
    try:
        qNames=[]
        for row in rows:
            qNames.append(row[0])
        return qNames
    except:
	    pass
    cursor.close()

def getqNames  ():
    try:
        cursor = cnxn.cursor()
        cursor.execute("SELECT csqname FROM RtCSQsSummary")
        rows = cursor.fetchall()
        qNamesAll=[]
        id=1
        for row in rows:
            qNamesAll.append({'id': id,'qname': row[0]})
            id=id +1
        returncode=200
        cursor.close()
    except pypyodbc.Error as ex:
        qNamesAll=[]
        sqlstate = ex.args[0]
        if sqlstate == 'HY000':
            print ('network connection issue')
        returncode=500
        pass
    return qNamesAll,returncode
    
	
def getQidFromName  (qname,qNames):
    try:
        for i in [i for i,x in enumerate(qNames) if x == qname]:
            return i+1
    except:
        return 404

def getAqStats  (id):
    qname= qNames[((int(id))-1)]
    cursor = cnxn.cursor()
    sql ='SELECT csqname,callswaiting,convoldestcontact,totalcalls,callsabandoned,callsdequeued,callshandled FROM RtCSQsSummary where csqname='+"'"+qname+"'"
    cursor.execute(sql)
    row = cursor.fetchone()
    try:
        qStats=[{'id': id,'qname': row[0],'callswaiting': row[1],'oldestwait': row[2], 'totalcalls': row[3],'abandonedcalls': row[4],'dequeuedcalls': row[5],'callshandled':row[6]}]
        return qStats
    except:
	    pass
    cursor.close()

def getAqStatsbyName  (qname,id):
    cursor = cnxn.cursor()
    sql ='SELECT csqname,callswaiting,convoldestcontact,totalcalls,callsabandoned,callsdequeued,callshandled FROM RtCSQsSummary where csqname='+"'"+qname+"'"
    cursor.execute(sql)
    row = cursor.fetchone()
    try:
        qStats=[{'id': id,'qname': row[0],'callswaiting': row[1],'oldestwait': row[2], 'totalcalls': row[3],'abandonedcalls': row[4],'dequeuedcalls': row[5],'callshandled':row[6]}]
        return qStats
    except:
	    pass
    cursor.close()

def getqStats  ():
    cursor = cnxn.cursor()
    cursor.execute("SELECT csqname,callswaiting,convoldestcontact,totalcalls,callsabandoned,callsdequeued,callshandled FROM RtCSQsSummary")
    rows = cursor.fetchall()
    try:
        qStatsAll=[]
        id=1
        for row in rows:
            qStatsAll.append({'id': id,'qname': row[0],'callswaiting': row[1],'oldestwait': row[2], 'totalcalls': row[3],'abandonedcalls': row[4],'dequeuedcalls': row[5],'callshandled':row[6]})
            id=id +1
        return qStatsAll
    except:
	    pass
    cursor.close()


@app.route('/screenshot/api/v1.0/queues', methods=['GET'])
def get_queuesRestCall():
    qNamesAll,returncode=getqNames()
    if returncode ==500:
        abort(500)
    elif len(qNamesAll) ==0:
        abort(404)
    else:
        return jsonify({'queues': qNamesAll})

@app.route('/screenshot/api/v1.0/queuestats', methods=['GET'])
def get_qStatsRestCall():
    qStatsAll=getqStats()
    return jsonify({'queues': qStatsAll})

@app.route('/screenshot/api/v1.0/queuestats/<int:q_id>', methods=['GET'])
def get_aQStatsRestCall(q_id):
    if (int(q_id)<1) or (int(q_id) >len(qNames)):
        abort(404)
    else:
        qStats=getAqStats(q_id)    
        if( len(qStats) == 0):
            abort(404)
        else:
            return jsonify({'queue': qStats})

@app.route('/screenshot/api/v1.0/queuestats/<qname>', methods=['GET'])
def get_aQStatsByQnameRestCall(qname):
    if not qname in qNames:
        abort(404)	
    else:
        id=getQidFromName(qname,qNames)
        qStats=getAqStatsbyName(qname,id)
        return jsonify({'queue': qStats})

if __name__ == '__main__':
    try:       
        
        while not bool(cnxn):
            try:
                print ('trying to connect UCCX DB')
                cnxn = pypyodbc.connect('DSN=UCCX')
                print ('DB connection Sucessful')				
            except pypyodbc.Error as ex:
                sqlstate = ex.args[0]
                if sqlstate == 'HY000':
                    print ('network connection timeout,retrying to connect to UCCX DB..')
                    pass
                else:
                    break	
        qNames = getQdetails  ()
        app.run(debug=False)		
    except KeyboardInterrupt:
        pass
    finally:
        print ("closing DB connection...")
        cnxn.close()    		

