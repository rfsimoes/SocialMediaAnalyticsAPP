import boto.cloudtrail.layer1
import json
import cPickle

conn = boto.cloudtrail.layer1.CloudTrailConnection(
    aws_access_key_id='AKIAIMWTUE6J5LGNZBMA',
    aws_secret_access_key='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')




LookupEvents = conn.lookup_events(lookup_attributes=None, start_time=None, end_time=None, max_results=1, next_token=None)


#### Buscar o username
#Username - tem o nome do user em string
Username = LookupEvents['Events'][0]['Username']

##### IR BUSCAR A DATA E HORA DO EVENTO
#Data - tem a data em string
#Hora - tem a hora em string

indice = LookupEvents['Events'][0]['CloudTrailEvent'].find('eventTime')+12
Tempo = LookupEvents['Events'][0]['CloudTrailEvent']
eventTime = Tempo[indice:indice+Tempo.find(',')-2]
Data = eventTime[0:eventTime.find('T')]
Hora = eventTime[eventTime.find('T')+1: eventTime.find('Z')]

####-------------------------------------------------------


##### IR BUSCAR O NOME DO EVENTO
#NomeEvento - tem o nome do evento em string

indice = LookupEvents['Events'][0]['CloudTrailEvent'].find('eventName')+12
Evento = LookupEvents['Events'][0]['CloudTrailEvent']
NomeEvento = Evento[indice:indice+Evento.find(',')]

####-------------------------------------------------------

print "Nome do evento: " + NomeEvento
print "Data: " + Data
print "Hora: " + Hora




