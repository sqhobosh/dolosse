import json
import time
import sys
import curses
from matplotlib import pyplot
import kafka



def populate(variables, decode,
             prepend=""):
    for other_key in decode:
        valid = True
        name = prepend+other_key
        try:
            following = float(decode[other_key])
        except (ValueError, TypeError):
            valid = False
        if isinstance(decode[other_key], dict):
            populate(variables,
                     decode[other_key],
                     name+": ")
        else:
            if name in variables:
                if valid:
                    variables[name].append(following)
                else:
                    variables[name].append(
                        variables[name][-1])
            else:
                variables[name] = [0]*(len(x_axis_data)-1)
                if valid:
                    variables[name].append(following)
                else:
                    variables[name].append(0)
                    #Remember, the list might still be empty here




topic = "default"

screen = curses.initscr()
curses.cbreak()
screen.nodelay(True)

if (len(sys.argv)) < 2:
    print("Please provide a topic name on the command line!")
    print("Using default topic name 'default'...")
else:
    topic = sys.argv[1]

data = {}
line = {}
plots = {}
figures = {}
axes = {}
x_axis_data = []
count = 0
pyplot.ion() #Interactive mode on
#pyplot.plot([1, 2, 3, 4], [4, 1, 3, 2])
# To consume latest messages and auto-commit offsets
consumer = kafka.KafkaConsumer(topic,
                               group_id='my-group',
                               bootstrap_servers=['localhost:9092'],
                               auto_offset_reset='earliest',
                               enable_auto_commit=False)
partition = kafka.TopicPartition(topic, 0)

exit_char = curses.ERR

for message in consumer:
    try:
        NewData = json.loads(message.value)
    except json.decoder.JSONDecodeError as problem:
        print("Error: Flawed JSON in value read from kafka topic: ",
              message.value)
        print("Error message: ", problem)
        NewData = ""
    data.update(NewData)
    print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                         message.offset, message.key,
                                         data))
    x_axis_data.append(count)
    count += 1

    populate(line, data)

    for key in line:
        while len(line[key]) < len(x_axis_data):
            line[key].append(0) #Variable went missing from data.
            #Buffer with zeroes for now.
        print(key, line[key])
        if key in plots:
            axes[key][0].set_ydata(line[key])
            axes[key][0].set_xdata(x_axis_data)
            plots[key].relim()
            plots[key].autoscale_view()
            figures[key].canvas.draw()
            figures[key].canvas.flush_events()
        else:
            figures[key], plots[key] = pyplot.subplots()
            axes[key] = plots[key].plot(line[key])
            plots[key].set_autoscaley_on(True)
            plots[key].set_autoscalex_on(True)
            plots[key].set_title(key)
    while (consumer.end_offsets([partition])[partition]
           == consumer.position(kafka.TopicPartition(topic, 0))):
        for key in line:
            figures[key].canvas.draw()
            figures[key].canvas.flush_events()
        time.sleep(0.2)
        exit_char = screen.getch()
        if exit_char != curses.ERR:
            break
    if exit_char != curses.ERR:
        break
    else:
        exit_char = screen.getch()



curses.nocbreak()
curses.endwin()
