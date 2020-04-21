"""Reads json data from a kafka topic and graphs it"""

from argparse import ArgumentParser
from curses import initscr, cbreak, ERR, nocbreak, endwin
from json import loads, decoder

from confluent_kafka import Consumer
from matplotlib import pyplot


def populate(variables, decode,
             count, prepend=""):
    """Takes the data from the dict and populates the variables"""
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
                     count,
                     name+": ")
        else:
            if name in variables:
                if valid:
                    variables[name].append(following)
                else:
                    variables[name].append(
                        variables[name][-1])
            else:
                variables[name] = [0]*(count-1)
                if valid:
                    variables[name].append(following)
                else:
                    variables[name].append(0)
                    #Remember, the list might still be empty here


def get_args():
    """Creates and returns a kafka consumer"""
    parser = ArgumentParser()
    parser.add_argument("--topic", default="default",
                        help="The name of the kafka topic from which to read")
    parser.add_argument("--group", default="my-group",
                        help="The kafka group in which the topic exists")
    parser.add_argument("--server", default="localhost:9092",
                        help="Name and port of the kafka server (e.g. localhost:9092)")
    arguments = parser.parse_args()

    # To consume latest messages and auto-commit offsets
    return arguments.topic, arguments.group, arguments.server

def redraw(figures):
    """Redraw all figures from a dict of figures"""
    for key in figures:
        figures[key].canvas.draw()
        figures[key].canvas.flush_events()


def update_data(data, value):
    """Update the list of data, given some json input"""
    try:
        update = loads(value)
    except decoder.JSONDecodeError as problem:
        print("Error: Flawed JSON in value: ",
              value)
        print("Error message: ", problem)
        update = ""
    data.update(update)


def graph(topic, group, server):
    """Main function - reads json data from a kafka topic and graphs it"""

    screen = initscr()
    cbreak()
    screen.nodelay(True)
    screen.getch()

    print("Generic Graphing Consumer")
    print("Press any key to exit...")

    data = {}
    line = {}
    plots = {}
    figures = {}
    axes = {}
    count = 0
    pyplot.ion() #Interactive mode on
    configuration = {"bootstrap.servers": server,
                     "group.id": group,
                     "default.topic.config" : {
                         "auto.offset.reset": "earliest"},
                     "enable.auto.commit": False}
    consumer = Consumer(configuration)
    consumer.subscribe([topic])
    keyboard_input = ERR

    while True:
        if keyboard_input != ERR:
            break
        keyboard_input = screen.getch()
        message = consumer.consume(num_messages=1, timeout=0.2)
        if (message is None) or (message == []):
            redraw(figures)
        elif message[0].error():
            print("Something went wrong with the read!")
            print(message.error())
        else:
            update_data(data, message[0].value())
            count += 1

            populate(line, data, count)

            for key in line:
                while len(line[key]) < count:
                    line[key].append(0) #Variable went missing from data.
                    #Buffer with zeroes for now.
                if key in plots:
                    axes[key][0].set_ydata(line[key])
                    axes[key][0].set_xdata(list(range(count)))
                    plots[key].relim()
                    plots[key].autoscale_view()
                else:
                    figures[key], plots[key] = pyplot.subplots()
                    axes[key] = plots[key].plot(line[key])
                    plots[key].set_autoscaley_on(True)
                    plots[key].set_autoscalex_on(True)
                    plots[key].set_title(key)

            redraw(figures)

    nocbreak()
    endwin()


def main_args():
    """Grabs parameters from the arguments"""
    topic_arg, group_arg, server_arg = get_args()
    graph(topic_arg, group_arg, server_arg)


if __name__ == '__main__':
    main_args()
