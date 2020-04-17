import kafka
import json
import time
from matplotlib import pyplot
import sys
import curses



def PopulateVariablesFromDict(CurrentVariableList, DictToDecode,
                              PrependString=""):
    for key in DictToDecode:
        ValidValue = True
        VariableName = PrependString+key
        try:
            NextValue = float(DictToDecode[key])
        except (ValueError, TypeError):
            ValidValue = False
        if (isinstance(DictToDecode[key], dict)):
            PopulateVariablesFromDict(CurrentVariableList, DictToDecode[key],
                                      VariableName+": ")
        else:
            if (VariableName in CurrentVariableList):
                if (ValidValue):
                    CurrentVariableList[VariableName].append(NextValue)
                else:
                    CurrentVariableList[VariableName].append(
                        CurrentVariableList[VariableName][-1])
            else:
                CurrentVariableList[VariableName] = [0]*(len(XData)-1)
                if (ValidValue):
                    CurrentVariableList[VariableName].append(NextValue)
                else:
                    CurrentVariableList[VariableName].append(0)
                    #Remember, the list might still be empty here




TopicName = "default"

Screen = curses.initscr()
curses.cbreak()
Screen.nodelay(True)

if (len(sys.argv))<2:
    print ("Please provide a topic name on the command line!")
    print ("Using default topic name 'default'...")
else:
    TopicName = sys.argv[1]

CurrentData = {}
LineData = {}
Plots = {}
Figures = {}
DataLines = {}
XData = []
Count = 0
pyplot.ion() #Interactive mode on
#pyplot.plot([1, 2, 3, 4], [4, 1, 3, 2])
# To consume latest messages and auto-commit offsets
consumer = kafka.KafkaConsumer(TopicName,
                               group_id='my-group',
                               bootstrap_servers=['localhost:9092'],
                               auto_offset_reset='earliest',
                               enable_auto_commit=False)
TestPartition = kafka.TopicPartition(TopicName, 0)

exit_char = curses.ERR

for message in consumer:
    try:
        NewData = json.loads(message.value)
    except json.decoder.JSONDecodeError as Problem:
        print ("Error: Flawed JSON in value read from kafka topic: ",
               message.value)
        print ("Error message: ", Problem)
        NewData = ""
    CurrentData.update(NewData)
    print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          CurrentData))
    XData.append(Count)
    Count+=1

    PopulateVariablesFromDict(LineData, CurrentData)

    for key in LineData:
        while (len(LineData[key]) < len(XData)):
            LineData[key].append(0); #Variable went missing from CurrentData.
            #Buffer with zeroes for now.
        print (key, LineData[key]);
        if (key in Plots):
            DataLines[key][0].set_ydata(LineData[key])
            DataLines[key][0].set_xdata(XData)
            Plots[key].relim()
            Plots[key].autoscale_view()
            Figures[key].canvas.draw()
            Figures[key].canvas.flush_events()
        else:
            Figures[key], Plots[key] = pyplot.subplots()
            DataLines[key] = Plots[key].plot(LineData[key])
            Plots[key].set_autoscaley_on(True)
            Plots[key].set_autoscalex_on(True)
            Plots[key].set_title(key)
    while (consumer.end_offsets([TestPartition])[TestPartition]
           == consumer.position(kafka.TopicPartition(TopicName, 0))):
        for key in LineData:
            Figures[key].canvas.draw()
            Figures[key].canvas.flush_events()
        time.sleep(0.2)
        exit_char = Screen.getch()
        if (exit_char != curses.ERR):
            break
    if (exit_char != curses.ERR):
        break
    else:
        exit_char = Screen.getch()



curses.nocbreak()
curses.endwin()
#    pyplot.show()

                

