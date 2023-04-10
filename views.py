from django.core.management.base import BaseCommand, CommandError
from portal.models import *
from django.conf import settings
from useraccount.models import *
from SMS.settings import BASE_DIR
# from django.core.management import setup_environ
# setup_environ(settings)
from django.db.models import Q
import pandas
from useraccount.models import *
from django.core.mail import EmailMultiAlternatives
import smtplib
import email_to
from django.db.models import Sum
import smpplib.client, smpplib.consts, smpplib.gsm
import sys
import io
from django.core.files import File
from django.views.decorators.csrf import csrf_exempt
from collections import defaultdict
from argparse import ArgumentParser
import re
from datetime import date
import csv
from io import StringIO
from django.core.files.base import ContentFile
import requests
import time
from .sendingStatistics import *
from manageAccounts.models import *


class Command(BaseCommand):
    help = 'Closes the specified poll for voting'

    def add_arguments(self, parser):
        # parser.add_argument('testDatabase_ids', nargs='+', type=int)
        pass

  

    def handle(self, *args, **options):
        time.sleep(15)
        proceed = False
        cron_job = cronJob.objects.filter(
            Q(cronJobId="econet1") & Q(sendingStatus="sending") & Q(status="on")
        ).last()

        if cron_job:
            if cron_job.scheduled == "on" and cron_job.scheduledTime:
                proceed = True
                if cron_job.startTime == "":
                    cron_job.startTime = datetime.now()
            else:
                proceed = True

        if proceed:
            cronJobx = cronJob.objects.filter(
                Q(cronJobId="econet1") & Q(sendingStatus="sending") & Q(status="on")
            ).last()
            tracker = MessageTracker.objects.get(id=cronJobx.trackerId)
            if sendJob.objects.filter(tracker=tracker.id).exists():
                job = sendJob.objects.get(tracker=tracker.id)
                if job.status == "sending":
                    count = 0
                    numbersCount = 0
                    indexTracker = []
                    updateTracker = 0
                    if int(cronJobx.total) > 0:
                        num = int(cronJobx.total) // 100 + 1
                    else:
                        cronJobx.status = "off"
                        cronJobx.sendingStatus = "completed"
                        cronJobx.save()

                    while (
                        count < num
                        and cronJobx.sendingStatus.lower() == "sending"
                        and cronJobx.status == "on"
                        and job.status != "completed"
                        and job.status != "canceled"
                    ):
                        count += 1
                        email = tracker.sender.email
                        error = False
                        try:
                            user = User.objects.get(email=email)
                        except User.DoesNotExist:
                            error = True

                        econetBalance = 0
                        if NormalAccount.objects.filter(email=email).exists():
                            subAcc = NormalAccount.objects.filter(email=email).last()
                            econetBalance = subAcc.totalBalance
                        else:
                            cronJobx.status = "off"
                            cronJobx.sendingStatus = "accountError"
                            cronJobx.save()
                            accountError(job, tracker)
                            return

                        balanceProceed = False
                        main = MainAccount.objects.all().last()
                        numBalanceProceed = 100 * float(main.currentPrice)
                        if int(cronJobx.total) > int(cronJobx.indexNo):
                            numBalanceProceedx = int(cronJobx.total) - int(cronJobx.indexNo)
                            if numBalanceProceedx > 100:
                                numBalanceProceed = 100 * float(main.currentPrice)
                            else:
                                numBalanceProceed = numBalanceProceedx * float(main.currentPrice)
                        if float(econetBalance) > numBalanceProceed:
                            balanceProceed = True

                            if balanceProceed and (int(cronJobx.indexNo) <= int(cronJobx.total)):
                                start = int(cronJobx.indexNo)
                                end = start + 100

                                masterSender = False
                                if start == 0:
                                    masterSender = True

                                if (start + 100) > int(cronJobx.total):
                                    end = int(cronJobx.total)

                                contactList = cronJobx.numbers.replace("\\", "").replace("'", "").replace('"', '').split(',')[start:end]

                                if contactList:
                                    range1 = f"{start},{end};"
                                    range2 = f"cron{cronJobx.id}{cronJobx.cronJobId[-1]}: {start},{end};"
                                    indexTracker.append(range2)

                                    if updateTracker == 0:
                                        sendSample(job, cronJobx, contactList, range2, "starting")
                                        updateTracker += 1
                                    elif updateTracker >= 50:
                                        sendSample(job, cronJobx, contactList, range2, indexTracker)
                                        updateTracker = 0
                                    else:
                                        updateTracker += 1

                                    if not worker.objects.filter(identifier=range2).exists():
                                        currentWorker = worker.objects.create(
                                            retry="true",
                                            provider="eco",
                                            job=job,
                                            status="pending",
                                            failed=range1,
                                            totalSuccess=0,
                                            totalFailed=len(contactList),
                                        )
                                        currentWorker.save()

                                        cronJobx.indexNo += len(contactList)
                                        cronJobx.save()

                                        xyx = self.hypereconet(job.message, contactList, job.senderID, cronJobx, job, currentWorker)
                                        numbersCount += len(contactList)

                                        subAcc.totalBalance -= float(xyx.totalSuccess) * float(tracker.msgDivision) * float(main.currentPrice)
                                        main.totalBalance -= float(xyx.totalSuccess) * float(tracker.msgDivision) * float(main.currentPrice)
                                        main.systemBalance -= float(xyx.totalSuccess) * float(tracker.msgDivision) * float(main.currentPrice)

                                        subAcc.save()
                                        main.save()

                            else:
                                if int(cronJobx.indexNo) >= int(cronJobx.total):
                                    cronJobx.status = "off"
                                    cronJobx.indexNo = cronJobx.total
                                    cronJobx.sendingStatus = "completed"
                                    cronJobx.save()
                                else:
                                    cronJobx.status = "off"
                                    cronJobx.sendingStatus = "out of balance"
                                    cronJobx.save()

                                    econetDepleted(job, tracker, "not finished")

                            if int(cronJobx.indexNo) >= int(cronJobx.total):
                                cronJobx.status = "off"
                                cronJobx.sendingStatus = "completed"
                                cronJobx.save()
                            else:
                                cronJobx.status = "on"
                                cronJobx.sendingStatus = "sending"
                                cronJobx.save()

                            econet111 = worker.objects.filter(job=job, provider="eco").aggregate(Sum("totalSuccess"))["totalSuccess__sum"]
                            econet222 = worker.objects.filter(job=job, provider="eco").aggregate(Sum("totalFailed"))["totalFailed__sum"]
                            netone111 = worker.objects.filter(job=job, provider="net").aggregate(Sum("totalSuccess"))["totalSuccess__sum"]
                            netone222  = worker.objects.filter(job=job,provider="net").aggregate(Sum('totalFailed'))['totalFailed__sum']
                            
                            if econet111:tracker.econetNo2 = econet111
                            if econet222:tracker.econetNo3 = econet222
                            if netone111:tracker.netoneNo2 = netone111
                            if netone222:tracker.netoneNo3 = netone222
                        
                        
                            tracker.totalSuccess = int(tracker.econetNo2) + int(tracker.netoneNo2) 
                            tracker.totalNumbers += numbersCount 
                            tracker.save()

                            if sendJob.objects.filter(tracker=tracker.id).exists():
                                job = sendJob.objects.get(tracker=tracker.id)
                                for i in range(1, 6):
                                    cron_job = cronJob.objects.get(trackerId=tracker.id, cronJobId=f"econet{i}")
                                    x = cron_job.indexNo
                                    tx = cron_job.total
                                    if float(tx) <= float(x):
                                        cron_job.indexNo = cron_job.total
                                        cron_job.sendingStatus = "completed"
                                        cron_job.save()
                                        x = cron_job.indexNo

                                t1 = sum(cronJob.objects.filter(trackerId=tracker.id, cronJobId__startswith="econet").values_list('indexNo', flat=True))
                                xyz1 = int(tracker.econetNo1) 
                                now = datetime.now()
                                if job.econetCron == "" and t1 >= xyz1:
                                    job.econetCron = str(now)
                                    job.save()
                                    cronJob.objects.filter(trackerId=tracker.id, cronJobId__startswith="econet").update(endTime=str(now))
                                    finishedEconet(job, tracker)

                                xyz = sum([int(tracker.econetNo2), int(tracker.netoneNo2), int(tracker.econetNo3), int(tracker.netoneNo3)])
                                tracker.allNo = xyz
                                num2 = int(tracker.econetNo1) + int(tracker.netoneNo1)
                                tracker.totalNumbers = xyz
                                tracker.totalSuccess = int(tracker.econetNo2) + int(tracker.netoneNo2)
                                tracker.save()
                                if job.allCron == "" and num2 <= xyz:
                                    finishedEmail(job, tracker, xyz)
                                    job.finalResult = "Successfully Completed"
                                    job.status = "Completed"
                                    job.allCron = str(now)
                                    job.save()
                                    tracker.save()
                                    job.save()
     
                else:
                    cronJobx.delete()
       

        else:
            if worker.objects.filter(retry="true", provider="eco", status="failed").exists():
                failed = worker.objects.filter(retry="true", provider="eco", status="failed")
                for currentWorker in failed:
                    job = currentWorker.job
                    cronJobx = cronJob.objects.get(trackerId=job.tracker)
                    contactList1 = getNumbers(job.id, currentWorker)
                    print(job.message, job.senderID, cronJobx, job, currentWorker, contactList1,)
                    try:
                        xyx = self.hypereconet(job.message, contactList1, job.senderID, cronJobx, job, currentWorker)
                        numbersCount = numbersCount + len(contactList1)
                        subAcc.totalBalance = float(subAcc.totalBalance) - (float(xyx.totalSuccess) * float(tracker.msgDivision) * float(main.currentPrice))
                        main.totalBalance = float(main.totalBalance) - (float(xyx.totalSuccess) * float(tracker.msgDivision) * float(main.currentPrice))
                        main.systemBalance = float(main.systemBalance) - (float(xyx.totalSuccess) * float(tracker.msgDivision) * float(main.currentPrice))
                        subAcc.save()
                        main.save()

                    except Exception as e:
                        print(e)
                        pass
                try:
                    econet111 = worker.objects.filter(job=job, provider="eco").aggregate(Sum('totalSuccess'))['totalSuccess__sum']
                    econet222 = worker.objects.filter(job=job, provider="eco").aggregate(Sum('totalFailed'))['totalFailed__sum']
                    netone111 = worker.objects.filter(job=job, provider="net").aggregate(Sum('totalSuccess'))['totalSuccess__sum']
                    netone222 = worker.objects.filter(job=job, provider="net").aggregate(Sum('totalFailed'))['totalFailed__sum']

                    if econet111:
                        tracker.econetNo2 = econet111
                    if econet222:
                        tracker.econetNo3 = econet222
                    if netone111:
                        tracker.netoneNo2 = netone111
                    if netone222:
                        tracker.netoneNo3 = netone222

                    tracker.totalSuccess = int(tracker.econetNo2) + int(tracker.netoneNo2)
                    tracker.totalNumbers = int(tracker.totalNumbers) + numbersCount
                    tracker.save()

                except Exception as e:
                    print(e)
                    pass
    
        
        for cronJobx in cronJob.objects.filter(Q(cronJobId= "econet1")): 
            if MessageTracker.objects.filter(id = cronJobx.trackerId ).exists():
                tracker = MessageTracker.objects.get(id = cronJobx.trackerId )
                
                if sendJob.objects.filter(tracker = tracker.id).exists():
                    job = sendJob.objects.get(tracker = tracker.id)
                    x1 = cronJob.objects.get(trackerId = tracker.id,cronJobId= "econet1").indexNo
                    x2 = cronJob.objects.get(trackerId = tracker.id,cronJobId= "econet2").indexNo
                    x3 = cronJob.objects.get(trackerId = tracker.id,cronJobId= "econet3").indexNo
                    x4 = cronJob.objects.get(trackerId = tracker.id,cronJobId= "econet4").indexNo
                    x5 = cronJob.objects.get(trackerId = tracker.id,cronJobId= "econet5").indexNo
                    t1 = int(x1) + int(x2) + int(x3) + int(x4) + int(x5)
                    xyz1 = int(tracker.econetNo1) 
                    now = datetime.now()
                    if job.econetCron == "":
                        if t1 >= xyz1:
                            job.econetCron = str(now)
                            job.save()
                            cronJobx.endTime = str(now)
                            cronJobx.save()
                            finishedEconet(job,tracker)

                    xyz = int(tracker.econetNo2) +  int(tracker.netoneNo2) + int(tracker.econetNo3) +  int(tracker.netoneNo3)
                    num2 = int(tracker.econetNo1) +  int(tracker.netoneNo1)
                    tracker.allNo = xyz
                    tracker.totalNumbers = xyz
                    tracker.totalSuccess = int(tracker.econetNo2) +  int(tracker.netoneNo2)
                    tracker.save()
                    if job.allCron == "":
                            if num2 <= xyz  :
                                finishedEmail(job,tracker,xyz)
                                job.finalResult = "Successfully Completed"
                                job.status = "Completed"
                                job.allCron = str(now)
                                job.save()
                               
                                
                tracker.save()
                job.save()
            else:
                cronJobx.delete()
                        
        getJob1 = cronJob.objects.filter(Q(cronJobId= "econet1") & Q(status = "on"))
        for x in getJob1:
            x.status = "off"
            x.save()                
                        
                        
                

def hypereconet(self, msgy, number, sender, cronJobx, job, currentWorker):
    self.stdout.write(self.style.SUCCESS(f'sending total "{len(number)}"'))
    
    numbers = ','.join(str(x) for x in number)
    senderID = sender
    msg = msgy
    xyz = currentWorker
    
    try:
        api_key = 'd15dbbf3dd55ff5d' if job.sendingOptions.lower() == 'live' else 'd15dbbf3dd5ff5d'
        url = f'https://mobilemessaging.econet.co.zw/client/api/sendmessage?apikey={api_key}&mobiles={numbers}&sms={msg}&senderid={senderID}'
        response = requests.post(url).json()
        
        xyz.status = 'sent'
        xyz.trials += 1
        xyz.number = f'cron{cronJobx.cronJobId[-1]} {xyz.failed}'
        xyz.totalSuccess = len(number)
        xyz.totalFailed = 0
        xyz.failed = ''
        xyz.save()
        workerRecognizer = xyz.number
        cronJobx.totalSuccess = len(number)
        cronJobx.save()
        return xyz
    
    except:
        retry_count = 0
        
        if job.sendingOptions.lower() == 'live':  
            while retry_count < 5:
                retyrFunction = self.hyperEconetRetry(msgy, number, sender, cronJobx, job, currentWorker)
                
                if retyrFunction != False:
                    sendSuccessfully(job, cronJobx, retry_count, number, 'success', workerRecognizer)
                    retry_count = 5
                    return retyrFunction
                
                sendSuccessfully(job, cronJobx, retry_count, number, 'fail', workerRecognizer)
                retry_count += 1
                
        xyz.status = 'failed'
        xyz.trials += 1
        xyz.totalFailed = len(number)
        xyz.failed = f'cron{cronJobx.cronJobId[-1]} {xyz.failed}'
        xyz.save()

        cronJobx.totalFailed = len(number)
        cronJobx.save()
        return xyz


def hyperEconetRetry(self, msgy, number, sender, cronJobx, job, currentWorker):
        
    self.stdout.write(self.style.SUCCESS('sending total retry "%s"' %len(number) ))
    numbersxy = number
    numbers = ','.join([str(x) for x in number])
    senderID = sender
    msg = msgy
    xyz = currentWorker
    
    for retryCount in range(5):
        try:
            if job.sendingOptions.lower() == "live":
                url = f'https://mobilemessaging.econet.co.zw/client/api/sendmessage?apikey=d15dbbf3dd55ff5d&mobiles={numbers}&sms={msg}&senderid={senderID}'
            else:
                url = f'https://mobilemessaging.econet.co.zw/client/api/sendmessage?apikey=d15dbbf3dd5ff5d&mobiles1={numbers}&sms={msg}&senderid=TestingAccountBk'
            response = requests.request("POST", url)
            response = response.json()
            xyz.status = "sent",
            xyz.trials = int(xyz.trials) + 1
            xyz.number = "cron" + cronJobx.cronJobId[-1] + " " + xyz.failed
            xyz.totalSuccess = len(numbersxy)
            xyz.totalFailed = 0
            xyz.failed = ""
            xyz.save()
            cronJobx.totalSuccess = len(numbersxy)
            cronJobx.save()
            return xyz
        except:
            if retryCount < 4:
                self.stdout.write(self.style.SUCCESS(f'Retrying attempt {retryCount+1} after 10 seconds'))
                time.sleep(10)
            else:
                self.stdout.write(self.style.SUCCESS(f'Failed after {retryCount+1} attempts'))
                xyz.status = "failed",
                xyz.trials = int(xyz.trials) + 1
                xyz.totalFailed = len(numbersxy)
                xyz.failed = "cron" + cronJobx.cronJobId[-1] + " " + xyz.failed
                xyz.save()

                cronJobx.totalFailed = len(numbersxy)
                cronJobx.save()
                return xyz

