import tensorflow as tf
from math import log, ceil
import numpy as np
from sklearn.model_selection import train_test_split
from tensorflow.python.keras.models import Model
from tensorflow.python.keras.layers import Input, Dense, Embedding, LSTM
from tensorflow.python.keras.layers.merge import concatenate
import google.cloud.bigquery as bq
import pandas as pd
from multiprocessing import Pool
import pandas_gbq
from Project2_CloudAuth import google_cloud_authenticate


# set project parameters to use in google cloud
PROJECT_ID = "bigdata-269209"
dataset_id = PROJECT_ID+".bdcc1920_project_datasets"
pd.set_option("display.max_columns", 101)


# obtain the number of distinct labels (vocabulary size)
def get_n_labels(ds_id, column):
    query = BQ_CLIENT.query(
        '''
        SELECT MAX(%s)+1 FROM `%s` 
        ''' % (column, ds_id))
    df = query.to_dataframe()

    return df.iloc[0, 0]


google_cloud_authenticate(PROJECT_ID)
BQ_CLIENT = bq.Client(PROJECT_ID)

# set table id for the preprocessed dataset
complete_encoded_ds_id = dataset_id+".CompleteEncodingICUMaterialized"
# get the number of labels (vocabulary size) for each of the variables we are going to encode in the neural network
itemIdNLabels = get_n_labels(complete_encoded_ds_id, "ENCODEDITEMID")
cgIdNLabels = get_n_labels(complete_encoded_ds_id, "ENCODEDCGID")
valueCatNLabels = get_n_labels(complete_encoded_ds_id, "ENCODEDVALUECAT")
valueUOMNLabels = get_n_labels(complete_encoded_ds_id, "ENCODEDVALUEUOM")

# set encoding layer for item id
itemIdEncoderInput = Input(shape=(1,))
itemIdEncoder = Embedding(itemIdNLabels, 16)(itemIdEncoderInput)
itemIdEncoder = Model(inputs=itemIdEncoderInput, outputs=itemIdEncoder)

# set encoding layer for gc id
cgIdEncoderInput = Input(shape=(1,))
cgIdEncoder = Embedding(cgIdNLabels, 8)(cgIdEncoderInput)
cgIdEncoder = Model(inputs=cgIdEncoderInput, outputs=cgIdEncoder)

# set encoding layer for valueCAT
valueCATEncoderInput = Input(shape=(1,))
valueCATEncoder = Embedding(valueCatNLabels, 4)(valueCATEncoderInput)
valueCATEncoder = Model(inputs=valueCATEncoderInput, outputs=valueCATEncoder)

# set encoding layer for valueUOM
valueUOMEncoderInput = Input(shape=(1,))
valueUOMEncoder = Embedding(valueUOMNLabels, 4)(valueUOMEncoderInput)
valueUOMEncoder = Model(inputs=valueUOMEncoderInput, outputs=valueUOMEncoder)

# obtain the data without the columns that either won't be used for training or will be encoded
query = BQ_CLIENT.query(
    '''
    SELECT * 
    except(SUBJECT_ID, HADM_ID, row_id, ENCODEDITEMID, ENCODEDCGID, ENCODEDVALUECAT, ENCODEDVALUEUOM, CHARTTIME)
    FROM `%s` LIMIT 1
    ''' % complete_encoded_ds_id)
df = query.to_dataframe()

# get number of columns in data
nCols = len(df.columns)
# set the input layer to recieve the rest of the data (data that wont be encoded)
inputLayer = Input(shape=(1, nCols))
inputLayer = Model(inputs=inputLayer, outputs=inputLayer)

# create merge layer, to combine the encoded layers and the input layer
fullInput = concatenate(
    [itemIdEncoder.output,
     cgIdEncoder.output,
     valueCATEncoder.output,
     valueUOMEncoder.output,
     inputLayer.output
     ])

# create the LSTM layer
fullInput = LSTM(2)(fullInput)
# create a dense(1) layer for output of a single value
fullInput = Dense(1)(fullInput)

# join all layers in a single model
model = Model(
    [itemIdEncoder.input,
     cgIdEncoder.input,
     valueCATEncoder.input,
     valueUOMEncoder.input,
     inputLayer.input
     ], outputs=fullInput)

# set model loss, optimizer and evaluation metrics
model.compile(loss='mean_squared_error', optimizer='adam', metrics=[tf.keras.metrics.RootMeanSquaredError()])

# load the unique hadm_ids
query = BQ_CLIENT.query(
    '''
    SELECT distinct HADM_ID  FROM `%s` order by 1
    ''' % complete_encoded_ds_id)
df = query.to_dataframe()

stays = df.values.tolist()

# split data into train, validation and test datasets
trainIndex, testIndex, = train_test_split(stays, test_size=0.33, random_state=1)
trainIndex, validationIndex, = train_test_split(trainIndex, test_size=0.1, random_state=1)


def parallel(i):
    # chunkSize must be multiple of nProcs
    procChunkSize = int(chunkSize/nProcs)

    # authenticate into google cloud for each thread
    google_cloud_authenticate(PROJECT_ID)
    THREAD_BQ = bq.Client(PROJECT_ID)

    # get the query indexes for each thread
    componentTrainIndex = trainIndex[(row_index + procChunkSize*i):min(row_index + procChunkSize*(i+1), len(trainIndex))]
    search_words_string_admission = str([element[0] for element in componentTrainIndex])[1:-1]

    # obtain the train data
    query = THREAD_BQ.query(
        '''
        SELECT * except(SUBJECT_ID, row_id) FROM `%s` WHERE HADM_ID in (%s) order by HADM_ID, CHARTTIME 
        ''' % (complete_encoded_ds_id, search_words_string_admission))
    df = query.to_dataframe()

    # obtain the train target (time spent in hospital)
    query = THREAD_BQ.query(
        '''
        SELECT HADM_ID, DAYS_SPENT  FROM `%s` WHERE HADM_ID in (%s) 
        ''' % (dataset_id + ".DaysSpentICU", search_words_string_admission))
    staydf = query.to_dataframe()

    return df, staydf


nEpochs = 1
# train for n epochs
for epoch in range(0, nEpochs):
    print("Epoch ", epoch+1, " out of ", nEpochs)

    # set number of processes and total size
    nProcs = 10
    chunkSize = 500

    procs = []
    results = []
    chunkSize = 1500

    r = None
    i = 0
    for row_index in range(0, len(trainIndex), chunkSize):

        # get initial data, synchronously
        if row_index == 0:
            current_row_index = row_index
            with Pool(processes=nProcs) as p:
                # split data query between multiple threads for better performance
                results = results + [p.map(parallel, list(range(min(nProcs, int((len(trainIndex) - row_index) / (int(chunkSize/nProcs))))) ))]
                p.close()
                p.join()

        # check if there is more data, get it while fitting is running
        if row_index + chunkSize * 2 < len(trainIndex):
            current_row_index = row_index + chunkSize
            with Pool(processes=nProcs) as async_p:
                # split data query between multiple threads for better performance
                r = async_p.map_async(parallel, list(range(min(nProcs, int((len(trainIndex) - (row_index + chunkSize)) / (int(chunkSize/nProcs)))))), callback=results.append)
                async_p.close()
                r.get()

        # process data from each process
        result_to_process = results[0]
        results = results[1:]
        # get the train data from the returned results
        df = pd.concat([element[0] for element in result_to_process if element != ()])
        # get the target data from the returned results
        staydf = pd.concat([element[1] for element in result_to_process if element != ()])
        # get train indexes
        hadm_df = np.unique(df['HADM_ID'].to_numpy())

        # train for all the indexes
        for current_hadm in hadm_df:
            # select the data for the current index
            current_df = df[df['HADM_ID'] == current_hadm]
            current_stay_df = staydf[staydf['HADM_ID'] == current_hadm]

            # get number of entries
            nrows = len(current_df)

            # remove unnecessary columns
            current_df = current_df.drop("HADM_ID", axis = 1)
            current_stay_df = current_stay_df.drop("HADM_ID", axis = 1)

            # get stay data from index
            itemId = current_df.pop("ENCODEDITEMID")
            cgId = current_df.pop("ENCODEDCGID")
            valueCat = current_df.pop("ENCODEDVALUECAT")
            valueUOM = current_df.pop("ENCODEDVALUEUOM")
            charttime = current_df.pop("CHARTTIME")
            stay = current_df

            # change stay shape
            stay = np.array(stay)
            stay = stay.reshape((nrows, 1, nCols))

            # obtain a list of target values
            # each hospital stay has one single total stay time, but training expects a list
            stayTime = np.array([current_stay_df.iloc[0, 0] for i in range(0, nrows)])

            # fit the Keras model on the dataset
            print("Processed ", i, " out of", len(trainIndex), " Hospital Stays")
            model.fit(
                [itemId, cgId, valueCat, valueUOM, stay],
                stayTime,
                epochs=1
            )
            i += 1
            model.reset_states()

        # get the new training data from the asynchronous requests
        if row_index + chunkSize * 2 < len(trainIndex):
            r.wait()
            async_p.join()

    # set number of processes and total size
    nProcs = 10
    chunkSize = 500

    procs = []
    results = []
    chunkSize = 1500
    results_dict = {}

    for row_index in range(0, len(validationIndex), chunkSize):
        print("Processed ", row_index, " out of", len(validationIndex), " Validation Hospital Stays")

        # get initial data, synchronously
        if row_index == 0:
            with Pool(processes=nProcs) as p:
                current_row_index = row_index
                # split data query between multiple threads for better performance
                results = results + [p.map(parallel, list(range(min(nProcs, int((len(trainIndex) - row_index) / (int(chunkSize/nProcs)))))))]
                p.close()
                p.join()

        # check if there is more data, get it while validation is running
        if row_index + chunkSize * 2 < len(validationIndex):
            current_row_index = row_index + chunkSize
            with Pool(processes=nProcs) as async_p:
                # split data query between multiple threads for better performance
                r = async_p.map_async(parallel, list(range(min(nProcs, int((len(validationIndex) - (row_index + chunkSize)) / (int(chunkSize/nProcs)))))), callback=results.append)
                async_p.close()
                r.get()

        # process data from each process
        result_to_process = results[0]
        results = results[1:]
        # get the validation data from the returned results
        df = pd.concat([element[0] for element in result_to_process if element != ()])
        # get the target data from the returned results
        staydf = pd.concat([element[1] for element in result_to_process if element != ()])
        # get validation indexes
        hadm_df = np.unique(df['HADM_ID'].to_numpy())

        for current_hadm in hadm_df:
            # select the data for the current index
            current_df = df[df['HADM_ID'] == current_hadm]
            current_stay_df = staydf[staydf['HADM_ID'] == current_hadm]

            # get number of entries
            nrows = len(current_df)

            # remove unnecessary columns
            current_df = current_df.drop("HADM_ID",axis = 1)
            current_stay_df = current_stay_df.drop("HADM_ID",axis = 1)

            # get stay data from index
            itemId = current_df.pop("ENCODEDITEMID")
            cgId = current_df.pop("ENCODEDCGID")
            valueCat = current_df.pop("ENCODEDVALUECAT")
            valueUOM = current_df.pop("ENCODEDVALUEUOM")
            charttime = current_df.pop("CHARTTIME")
            stay = current_df

            # change stay shape
            stay = np.array(stay)
            stay = stay.reshape((nrows, 1, nCols))

            # obtain a list of target values
            # each hospital stay has one single total stay time, but training expects a list
            stayTime = np.array([current_stay_df.iloc[0, 0] for i in range(0, nrows)])

            print("Validation results per window, format = window: metric")
            j = 0
            prevWindowSize = 0
            for j in range(0, ceil(log(nrows, 2))):
                windowSize = 2 ** j

                # get stay data for current window size
                itemIdWindow = itemId.values[prevWindowSize:windowSize]
                cgIdWindow = cgId.values[prevWindowSize:windowSize]
                valueCatWindow = valueCat.values[prevWindowSize:windowSize]
                valueUOMWindow = valueUOM.values[prevWindowSize:windowSize]
                stayWindow = stay[prevWindowSize:windowSize, :, :]
                stayTimeWindow = stayTime[prevWindowSize:windowSize]

                # evaluate data with current window size
                resultsWindow = model.evaluate(
                    [itemIdWindow, cgIdWindow, valueCatWindow, valueUOMWindow, stayWindow],
                    stayTimeWindow, verbose = 0
                )
                print(windowSize, ":", resultsWindow, end=" ")
                prevWindowSize = windowSize

                # save results
                if windowSize in results_dict.keys():
                    results_dict[windowSize].append(resultsWindow[1])
                else:
                    results_dict[windowSize] = [resultsWindow[1]]
                    
            model.reset_states()

        # get the new validation data from the asynchronous requests
        if row_index + chunkSize * 2 < len(validationIndex):
            r.wait()
            async_p.join()
    print("Mean error per window size")
    for windowSize in results_dict.keys():
        print(windowSize, ":", sum(results_dict[windowSize])/len(results_dict[windowSize]))



