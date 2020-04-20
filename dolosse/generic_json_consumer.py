"""Reads json data from a kafka topic and graphs it"""

from argparse import ArgumentParser
from json import loads, decoder
from time import sleep

from curses import initscr, cbreak, ERR, nocbreak, endwin
from kafka import KafkaConsumer, TopicPartition
from matplotlib import pyplot


def populate(variables, decode,
             x_axis_data, prepend=""):
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
                     x_axis_data,
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


def create():
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
    consumer = KafkaConsumer(arguments.topic,
                             group_id=arguments.group,
                             bootstrap_servers=[arguments.server],
                             auto_offset_reset='earliest',
                             enable_auto_commit=False)
    return consumer

def redraw(figures, key):
    """Redraw the given figure from a dict of figures"""
    figures[key].canvas.draw()
    figures[key].canvas.flush_events()


def main():
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
    x_axis_data = []
    count = 0
    pyplot.ion() #Interactive mode on
    # To consume latest messages and auto-commit offsets
    consumer = create()
    exit_char = ERR

    for message in consumer:
        try:
            update = loads(message.value)
        except decoder.JSONDecodeError as problem:
            print("Error: Flawed JSON in value read from kafka topic: ",
                  message.value)
            print("Error message: ", problem)
            update = ""
        data.update(update)
        x_axis_data.append(count)
        count += 1

        populate(line, data, x_axis_data)

        for key in line:
            while len(line[key]) < len(x_axis_data):
                line[key].append(0) #Variable went missing from data.
                #Buffer with zeroes for now.
            if key in plots:
                axes[key][0].set_ydata(line[key])
                axes[key][0].set_xdata(x_axis_data)
                plots[key].relim()
                plots[key].autoscale_view()
                redraw(figures, key)
            else:
                figures[key], plots[key] = pyplot.subplots()
                axes[key] = plots[key].plot(line[key])
                plots[key].set_autoscaley_on(True)
                plots[key].set_autoscalex_on(True)
                plots[key].set_title(key)
        while (consumer.end_offsets([TopicPartition(message.topic, 0)])
               [TopicPartition(message.topic, 0)]
               == consumer.position(TopicPartition(message.topic, 0))):
            for key in line:
                redraw(figures, key)
            sleep(0.2)
            exit_char = screen.getch()
            if exit_char != ERR:
                break
        if exit_char != ERR:
            break
        exit_char = screen.getch()

    nocbreak()
    endwin()


if __name__ == '__main__':
    main()