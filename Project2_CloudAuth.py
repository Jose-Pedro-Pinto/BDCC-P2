# authenticate to google cloud
def google_cloud_authenticate(projectId, keyFile=None, debug=True):
    import os
    if keyFile == None:
        keyFile='/content/bdcc-colab.json'
    if os.access(keyFile,os.R_OK):
        if debug:
            print('Using key file "%s"' % keyFile)
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '%s' % keyFile
        os.environ['GCP_PROJECT'] = projectId
        os.environ['GCP_ACCOUNT'] = 'bdcc-colab@' + projectId + '.iam.gserviceaccount.com'
        os.system('gcloud auth activate-service-account "$GCP_ACCOUNT" --key-file="$GOOGLE_APPLICATION_CREDENTIALS" --project="$GCP_PROJECT"')
    os.system('gcloud info | grep -e Account -e Project')