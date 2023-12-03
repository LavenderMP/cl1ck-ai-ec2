import pandas as pd
import openai
import boto3
import os
from io import StringIO
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib

load_dotenv()

s3Client = boto3.client(
    's3',
    aws_access_key_id=os.environ.get('AWS_ACCSS_KEY_ID'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_KEY')
)
s3Resource = boto3.resource( 
    's3',
    aws_access_key_id=os.environ.get('AWS_ACCSS_KEY_ID'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_KEY')
)


def fix_website_url(url):
    if url.startswith('http://') or url.startswith('https://'):
        return url
    else:
        return 'http://' + url
    
def generate_response_gpt35(prompt):

    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        
        messages=[
                {"role": "system", 
                 "content": "You can filter thru a scrapped website to retrieve company's description"
                },           
                {"role": "user", 
                 "content": prompt
                },
            ],
    temperature=0.5,
    max_tokens=100,
    n=2,
    )
    result = ''
    for choice in response.choices:
        result += choice.message.content
    return result

def email_sender(body):
    # Set up the SMTP server
    smtp_server = "smtp.office365.com"
    port = 587  # For starttls
    sender_email = "noreply@cl1ck.biz"
    password = "Techteam$2021"
    receiver_email = body.get("email", "")
    experiment = body.get("exp", "")
    isError = body.get("isError", False)
    msgerr = body.get("msg_err", '')
    print('body', body)


    # Create a multipart message and set headers
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = body.get("subject", f"Result of processing lead {experiment}")

    # Add body to email
    if isError:
        content = body.get("content", f"Processing lead of {experiment} is undone please contract with Huynh/Monesh, error {msgerr}")
    else:
        content = body.get("content", f"Processing lead of {experiment} is successfully")
    message.attach(MIMEText(content, "html"))

    # Convert message to string
    text = message.as_string()

    # Try to log in to server and send email
    try:
        server = smtplib.SMTP(smtp_server, port)
        server.ehlo()  # Can be omitted
        server.starttls()  # Secure the connection
        server.ehlo()  # Can be omitted
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, text)
        response = {
            "status": "success",
        }
    except Exception as e:
        # Print any error messages to the console
        response = {"status": "error", "message": str(e)}
    finally:
        server.quit()
        return response

def processLeads(experiment, receivedEmail='', personalized=True, filePath = None, classifyOnly=False):
    try:
        if filePath is None:
            s3_object = s3Client.get_object(Bucket ='cl1ckmediabucket', Key='EmailExperimentAnalysis/Leads/'+str(experiment)+'_master.csv')
            try:
                leads_df = pd.read_csv(s3_object['Body'])
            except:
                leads_df = pd.read_csv(s3_object['Body'], encoding="ISO-8859-1")
            leads_df = leads_df.loc[:, ~leads_df.columns.str.contains('Unnamed')]
        else:
            leads_df = pd.read_csv(filePath, delimiter=";")

        openai.api_key = "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"
        if "Classified Loc" not in leads_df:
            leads_df["Classified Loc"] = ""
        if "Classified Role" not in leads_df:
            leads_df["Classified Role"] = ""
        if "sentContent" not in leads_df:
            leads_df['sentContent'] = ""
        if "sentHeader" not in leads_df:
            leads_df['sentHeader'] = ""
        leads_df['sentContent']=leads_df['sentContent'].astype(str)
        leads_df['Company Domain']=leads_df['Company Domain'].astype(str)
        leads_df['sentHeader']=leads_df['sentHeader'].astype(str)
        leads_df['Classified Loc']=leads_df['Classified Loc'].astype(str)
        leads_df['Classified Role']=leads_df['Classified Role'].astype(str)

        bucket = s3Resource.Bucket('cl1ckmediabucket')   
        sentInfo = [f.key for f in bucket.objects.filter(Prefix="EmailExperimentAnalysis/SentInfo/").all()]

        if 'Company Desc' not in leads_df:
            leads_df['Company Desc'] = ""
        leads_df['Company Desc']=leads_df['Company Desc'].astype(str)
        leads_df['Person Location'].fillna("", inplace=True)

        
        if 'Sent' not in leads_df.columns:
            leads_df['Sent'] = False

        if 'Best Email' not in leads_df.columns:
            leads_df['Best Email'] = 'test@gmail.com'
        else:
            leads_df['Best Email'].fillna('test@gmail.com', inplace=True)

        leads_df['Person Location'].fillna("", inplace=True)
        leads_df['Person Job Title'].fillna("", inplace=True)
        uniqueCompanyDesc = {}
        dfLen = len(leads_df)
        for index, row in leads_df.iterrows():
            print("Lead " + str(index+1) + "/" + str(dfLen))
            if row['Sent']:
                continue
            if not row['Sent']:
                if personalized:
                    if row['Company Desc'] == "" or pd.isnull(row['Company Desc']) or row['Company Desc']=="nan":
                        print("Performing company scrape for unsent lead")
                        if pd.isnull(row['Company Domain']) or row['Company Domain']=="nan":
                            if "gmail" in row['Best Email'] or "yahoo" in row['Best Email'] or "outlook" in row['Best Email'] or "hotmail" in row['Best Email'] or "test.com" in row['Best Email']:
                                continue
                            domain = row['Best Email'].split('@')[1]
                            leads_df.at[index,'Company Domain'] = domain
                        else: domain = row['Company Domain']
                        print("domain", domain)
                        if domain in uniqueCompanyDesc:
                            leads_df.at[index,"Company Desc"] = uniqueCompanyDesc[domain]
                        else:
                            try:
                                header = {
                                    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36" ,
                                    'referer':'https://www.google.com/'
                                }
                                response = requests.get(fix_website_url(domain), headers=header, timeout=20)
                                if response.status_code == 200:
                                    soup = BeautifulSoup(response.content, 'html.parser')
                                    # Extract all the text from the webpage
                                    text = soup.get_text()
                                    text = text.replace('\n', '')
                                    trimText = text[:4000] if len(text)>4000 else text
                                    prompt = '''
                                    Given this company website information, which has been scrapped from the company's website:
                                    ''' + trimText +\
                                    '''                    
                                    
                                    Please search for the company's description and only return the description, 
                                    omitting other unnecessary information.
                                    Please return the result in English. 
                                    Please translate non-English language into English.
                                    '''
                                    response_gpt35 = generate_response_gpt35(prompt)
                                    leads_df.at[index,"Company Desc"] = response_gpt35
                                    uniqueCompanyDesc[domain] = response_gpt35
                                    print("Added Company Desc")
                                else:
                                    print(f"Error {response.status_code}: Unable to fetch and process content from {domain}")

                            except requests.Timeout:
                                print(f"Timeout occurred while fetching content from {domain}, moving to the next iteration")

                            except Exception as e:
                                print(f"Error occurred while scraping {domain}: {e}")
                        print("Scraping for unsent lead done")
            if row['sentContent']=="nan" or row['sentHeader']=="nan" or row['sentContent']=="" or row['sentHeader']=="":
                print("Getting Sent info for lead")
                try:
                    for sentKey in sentInfo:
                        if experiment not in sentKey or "fu_" in sentKey:
                            continue
                        try:
                            sentObj = s3Client.get_object(Bucket ='cl1ckmediabucket', Key=sentKey)
                            sentDf = pd.read_csv(sentObj['Body'], delimiter =",")
                        except:                
                            sentObj = s3Client.get_object(Bucket ='cl1ckmediabucket', Key=sentKey)
                            sentDf = pd.read_csv(sentObj['Body'], delimiter =";")
                        if len(sentDf[sentDf['email']==row['Best Email']]) == 0:
                            continue
                        sentContent = sentDf.loc[sentDf['email']==row['Best Email'], "style"].values[0]
                        leads_df.at[index, "sentContent"] = sentContent                
                        sentHeader = sentDf.loc[sentDf['email']==row['Best Email'], "header_type"].values[0]
                        leads_df.at[index, "sentHeader"] = sentHeader
                    print("Sent info for lead done")
                except:
                    print("Sent info for lead done")
            if row['Classified Loc']=="nan" or row['Classified Role']=="nan" or row['Classified Loc']=="" or row['Classified Role']=="":
                print("Demo Classifying for lead")
                location = row['Person Location']
                position = row['Person Job Title']
                locationCatVal = "N/A"
                positionCatVal = "3"
                try:
                    if location:
                        locationResp = openai.Completion.create(
                            model="text-davinci-003",
                            prompt="Provide a country name that is most associated with this text:"+location,
                            max_tokens=5,
                            temperature=0
                            )
                        locationCatVal = locationResp['choices'][0]['text'].strip()
                    if position:
                        positionsResp = openai.Completion.create(
                            model="text-davinci-003",
                            prompt="Provide a number from 1 to 5 based on how high-ranking the position: "+position+" is where 1 is the lowest ranking and 5 is the highest ranking",
                            max_tokens=3,
                            temperature=0
                            )
                        positionCatVal = positionsResp['choices'][0]['text'].strip()
                except Exception as err:
                    print(f"Error at converting OpenAI: {err}")

                if "1" in str(positionCatVal):
                    positionCatVal="1"
                elif "2" in str(positionCatVal):
                    positionCatVal="2"
                elif "3" in str(positionCatVal):
                    positionCatVal="3"
                elif "4" in str(positionCatVal):
                    positionCatVal="4"
                elif "5" in str(positionCatVal):
                    positionCatVal="5"
                else:
                    positionCatVal = "3"
                leads_df.at[index, "Classified Loc"] = locationCatVal
                leads_df.at[index, "Classified Role"] = positionCatVal
                print("Demo classification for lead done for exp" + experiment)

                csv_buffer = StringIO()
                leads_df.to_csv(csv_buffer)
                bucket = 'cl1ckmediabucket' # already created on S3
                s3Resource.Object(bucket, 'EmailExperimentAnalysis/Leads/'+str(experiment)+'_master.csv').put(Body=csv_buffer.getvalue())
        
        if receivedEmail:
            print('Sending email')
            email_sender({
                "email": receivedEmail,
                "isError": False,
                "exp": experiment
            })
        print("Done process leads")

    except Exception as err:
        if receivedEmail:
            print('Sending email')
            email_sender({
                "email": receivedEmail,
                "isError": True,
                "exp": experiment,
                "msg_err": err
            })