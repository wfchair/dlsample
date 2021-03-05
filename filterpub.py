from gstgva import util
import sys
import json
import numpy
import cv2
from argparse import ArgumentParser

import gi
gi.require_version('GObject', '2.0')
gi.require_version('Gst', '1.0')
gi.require_version('GstApp', '1.0')
gi.require_version('GstVideo', '1.0')
from gi.repository import Gst, GLib, GstApp, GstVideo

parser = ArgumentParser(add_help=False)
_args = parser.add_argument_group('Options')
_args.add_argument("-d", "--detection_model", help="Required. Path to an .xml file with object detection model",
                   required=True, type=str)
_args.add_argument("-p", "--proc",
                   help="Required. Path to an .json file with model_proc",
                   required=True, type=str)
_args.add_argument("-c", "--cls",
                   help="Required. object class whose msg to publish",
                   required=True, type=str)
_args.add_argument("-a", "--mqtt_addr",
                   help="Required. mqtt address (e.g.:localhost:1883) where to publish",
                   required=True, type=str)
_args.add_argument("-t", "--mqtt_topic",
                   help="Required. mqtt topic",
                   required=True, type=str)
args = parser.parse_args()


def pad_probe_callback(pad, info):
    with util.GST_PAD_PROBE_INFO_BUFFER(info) as buffer:
        for json_meta in util.GVAJSONMeta.iterate(buffer):
            msg = json.loads(json_meta.get_message())
            if args.cls != msg['objects'][0]['roi_type']:
            #if args.cls not in json_meta.get_message():
                util.GVAJSONMeta.remove_json_meta(buffer, json_meta.get_message().meta)
                break
    
    return Gst.PadProbeReturn.OK


def create_launch_string():
    return "v4l2src ! decodebin ! \
    video/x-raw ! videoconvert ! \
    gvadetect model={} model_proc={} ! queue ! \
    gvametaconvert format=json ! \
    gvametapublish name=gvametapublish method=mqtt address={} topic={} ! \
    fakesink".format(args.detection_model, args.proc, args.mqtt_addr, args.mqtt_topic)


def glib_mainloop():
    mainloop = GLib.MainLoop()
    try:
        mainloop.run()
    except KeyboardInterrupt:
        pass


def bus_call(bus, message, pipeline):
    t = message.type
    if t == Gst.MessageType.EOS:
        print("pipeline ended")
        pipeline.set_state(Gst.State.NULL)
        sys.exit()
    elif t == Gst.MessageType.ERROR:
        err, debug = message.parse_error()
        print("Error:\n{}\nAdditional debug info:\n{}\n".format(err, debug))
        pipeline.set_state(Gst.State.NULL)
        sys.exit()
    else:
        pass
    return True


def set_callbacks(pipeline):
    gvametapub = pipeline.get_by_name("gvametapublish")
    pad = gvametapub.get_static_pad("sink")
    pad.add_probe(Gst.PadProbeType.BUFFER, pad_probe_callback)

    bus = pipeline.get_bus()
    bus.add_signal_watch()
    bus.connect("message", bus_call, pipeline)


if __name__ == '__main__':
    Gst.init(sys.argv)
    gst_launch_string = create_launch_string()
    print(gst_launch_string)
    pipeline = Gst.parse_launch(gst_launch_string)

    set_callbacks(pipeline)

    pipeline.set_state(Gst.State.PLAYING)

    glib_mainloop()

    print("Exiting")
