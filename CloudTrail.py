import boto.cloudtrail.layer1
import json
import cPickle

#Declaracao de variaveis
MAX_RESULTS =50
i = 0
#Fim declaracao

print "----A realizar conexao a Cloud para obter logs----"

conn = boto.cloudtrail.layer1.CloudTrailConnection(
    aws_access_key_id='AKIAIMWTUE6J5LGNZBMA',
    aws_secret_access_key='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')

print "----A obter logs----"
LookupEvents = conn.lookup_events(lookup_attributes=None, start_time=None, end_time=None, max_results=MAX_RESULTS, next_token=None)

print "----A preparar para escrever logs----"
print
print

try:
    for i in range(0,MAX_RESULTS):
        aux = LookupEvents['Events'][i]
        print "----Log: " + str(i+1) + " ----"
        #### Buscar o username
        #Username - tem o nome do user em string
        Username = LookupEvents['Events'][i]['Username']

        ##### IR BUSCAR A DATA E HORA DO EVENTO
        #Data - tem a data em string
        #Hora - tem a hora em string
        indice = LookupEvents['Events'][i]['CloudTrailEvent'].find('eventTime')+12
        Tempo = LookupEvents['Events'][i]['CloudTrailEvent']
        eventTime = Tempo[indice:indice+Tempo.find(',')-2]
        Data = eventTime[0:eventTime.find('T')]
        Hora = eventTime[eventTime.find('T')+1: eventTime.find('Z')]
        ####-------------------------------------------------------


        ##### IR BUSCAR O NOME DO EVENTO
        #NomeEvento - tem o nome do evento em string
        indice = LookupEvents['Events'][i]['CloudTrailEvent'].find('eventName')+12
        Evento = LookupEvents['Events'][i]['CloudTrailEvent']
        NomeEvento = Evento[indice:indice+Evento.find(',')]
        NomeEvento = NomeEvento[0:NomeEvento.find(',') - 1]
        ####-------------------------------------------------------

        ##### Regiao
        #NomeRegiao - tem o nome da regiao em string
        indice = LookupEvents['Events'][i]['CloudTrailEvent'].find('awsRegion')+12
        Regiao = LookupEvents['Events'][i]['CloudTrailEvent']
        NomeRegiao = Regiao[indice:indice+Regiao.find(',')]
        NomeRegiao = NomeRegiao[0:NomeRegiao.find(',') - 1]
        ####-------------------------------------------------------

        print "Username: " + Username
        print "Regiao: " + NomeRegiao
        print "Nome do evento: " + NomeEvento
        print "Data: " + Data
        print "Hora: " + Hora
        print "----Fim do log: " + str(i+1) + " ----"
        print
        print
except:
    print "Fim de programa com " + str(i) + " logs escritos"