"""Reads json data from a kafka topic and graphs it"""

from argparse import ArgumentParser
from curses import initscr, cbreak, ERR, nocbreak, endwin
from json import loads, decoder

from confluent_kafka import Consumer
from matplotlib import pyplot


def populate(variables, decode, count, prepend=""):
    """Takes the data from the dict and populates the variables

    Parameters:
        variables: Python dict, either empty or containing a number of variables
                   organised with variable name as the key and a list of values
                   over time as the value. Modified by this function.
        decode: Dictionary containing variable data at the new time interval,
                as decoded directly from the JSON message
        count: Number of time intervals that have already elapsed
        prepend: String with which to prepend all variable names. Only used when
                 recursing; let this take the default value when calling this function
                 from anywhere outside this function. If that is somehow impossible,
                 then use the same value every time.

    Return value: No direct return; this function rather populates the 'variables' dict
                  by appending new data from the 'decode' dict.
    """
    for other_key in decode.keys():
        valid = True
        name = prepend + other_key
        try:
            following = float(decode[other_key])
        except (ValueError, TypeError):
            valid = False
        if not valid:
            try:
                following = int(decode[other_key], 16)  # Catch hexadecimals
                valid = True
            except (ValueError, TypeError):
                valid = False
        if isinstance(decode[other_key], dict):
            populate(variables,
                     decode[other_key],
                     count,
                     name + ": ")
        else:
            if name in variables:
                if valid:
                    variables[name].append(following)
                else:
                    variables[name].append(
                        variables[name][-1])
            else:
                variables[name] = [0] * (count - 1)
                if valid:
                    variables[name].append(following)
                else:
                    variables[name].append(0)
                    # Remember, the list might still be empty here


def get_args():
    """Creates and returns a kafka consumer

    Return value: A dict containing the topic name, group name, and server name
                  suitable for passing as a parameter to the graph function.
    """
    parser = ArgumentParser(description=
                            "A generic consumer that graphs any JSON data that it consumes")
    parser.add_argument("--topic", default="default",
                        help="The name of the kafka topic from which to read")
    parser.add_argument("--group", default="my-group",
                        help="The kafka group in which the topic exists")
    parser.add_argument("--server", default="localhost:9092",
                        help="Name and port of the kafka server (e.g. localhost:9092)")

    # To consume latest messages and auto-commit offsets
    return parser.parse_args()


def redraw(figures):
    """Redraw all figures from a dict of figures. Call often.

    Parameters:
    figures: A dict containing a list of figures to redraw.

    Return value: None. Merely redraws the figures in the list.
    """
    for key in figures.keys():
        figures[key].canvas.draw()
        figures[key].canvas.flush_events()


def update_data(data, value):
    """Update the list of data, given some json input

    Parameters:
    data: A dict containing a list of the variables being plotted, and their current values
    value: A string containing the latest JSON input received.

    Return value: No direct return. Rather, the variable data is updated with the values
                  found in value. And variables not found in value retain their previous values.
    """
    try:
        update = loads(value)
    except decoder.JSONDecodeError as problem:
        print("Error: Flawed JSON in value: ", value)
        print("Error message: ", problem)
        update = ""
    data.update(update)


def graph(arguments):
    """Main function - reads json data from a kafka topic and graphs it

    Parameters:
    arguments: Command-line arguments as a dict. Contains the following:
        topic: String containing the name of the kafka topic from which to read data
        group: String containing the group id to use when reading from kafka
        server: String containing the address of the kafka server

    Return value: None; this function plots the data provided by the named kafka topic,
                  as read by the named group from the named server, and plots it to screen
                  so that the user can see it. It ends when given a keypress.
    """

    screen = initscr()
    cbreak()
    screen.nodelay(True)
    screen.getch()

    print("Generic Graphing Consumer")
    print("Press any key to exit...")

    data = {}
    curve = {}
    plots = {}
    figures = {}
    axes = {}
    count = 0
    pyplot.ion()  # Interactive mode on
    consumer = Consumer({"bootstrap.servers": arguments.server,
                         "group.id": arguments.group,
                         "default.topic.config": {
                             "auto.offset.reset": "earliest"},
                         "enable.auto.commit": False})
    consumer.subscribe([arguments.topic])
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

            populate(curve, data, count)

            for key in curve.keys():
                while len(curve[key]) < count:
                    curve[key].append(0)  # Variable went missing from data.
                    # Buffer with zeroes for now.
                if key in plots.keys():
                    axes[key][0].set_ydata(curve[key])
                    axes[key][0].set_xdata(list(range(count)))
                    plots[key].relim()
                    plots[key].autoscale_view()
                else:
                    figures[key], plots[key] = pyplot.subplots()
                    axes[key] = plots[key].plot(curve[key])
                    plots[key].set_autoscaley_on(True)
                    plots[key].set_autoscalex_on(True)
                    plots[key].set_title(key)

            redraw(figures)

    nocbreak()
    endwin()


if __name__ == '__main__':
    graph(get_args())
