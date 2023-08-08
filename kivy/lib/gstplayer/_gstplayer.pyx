import datetime

from libcpp cimport bool
from weakref import ref
import atexit

cdef extern from 'gst/gst.h':
    ctypedef void *GstPipeline
    ctypedef void *GstElement
    ctypedef void *GstBus
    ctypedef void *GstPad
    ctypedef void *GstSample
    ctypedef void *GstBin
    ctypedef void (*appcallback_t)(void *, int, int, char *, int)
    ctypedef void (*buscallback_t)(void *, GstMessage *)
    ctypedef unsigned int guint
    ctypedef unsigned long gulong
    ctypedef void *gpointer
    ctypedef char const_gchar 'const gchar'
    ctypedef long int gint64
    ctypedef unsigned long long GstClockTime
    ctypedef int gboolean

    ctypedef enum GstState:
        GST_STATE_VOID_PENDING
        GST_STATE_NULL
        GST_STATE_READY
        GST_STATE_PAUSED
        GST_STATE_PLAYING

    ctypedef enum GstFormat:
        GST_FORMAT_TIME

    ctypedef enum GstSeekFlags:
        GST_SEEK_FLAG_KEY_UNIT
        GST_SEEK_FLAG_FLUSH

    ctypedef enum GstStateChangeReturn:
        pass

    ctypedef struct GError:
        int code
        char *message

    ctypedef enum GstMessageType:
        GST_MESSAGE_EOS
        GST_MESSAGE_ERROR
        GST_MESSAGE_WARNING
        GST_MESSAGE_INFO
        GST_MESSAGE_ELEMENT
        GST_MESSAGE_SEGMENT_START
        GST_MESSAGE_SEGMENT_DONE


    ctypedef struct GstMessage:
        GstMessageType type

    int GST_SECOND
    bool gst_init_check(int *argc, char ***argv, GError **error)
    bool gst_is_initialized()
    void gst_deinit()
    void gst_version(guint *major, guint *minor, guint *micro, guint *nano)
    GstElement *gst_element_factory_make(const_gchar *factoryname, const_gchar *name)
    bool gst_bin_add(GstBin *bin, GstElement *element)
    bool gst_bin_remove(GstBin *bin, GstElement *element)
    void gst_object_unref(void *pointer) nogil
    GstElement *gst_pipeline_new(const_gchar *name)
    GstElement *gst_parse_launch(const_gchar *pipeline_description, GError **error)
    GstElement *gst_bin_get_by_name(GstBin *bin, const_gchar *name);
    void gst_bus_enable_sync_message_emission(GstBus *bus)
    GstBus *gst_pipeline_get_bus(GstPipeline *pipeline)
    GstStateChangeReturn gst_element_get_state(
            GstElement *element, GstState *state, GstState *pending,
            GstClockTime timeout) nogil
    GstStateChangeReturn gst_element_set_state(
            GstElement *element, GstState state) nogil
    void g_signal_emit_by_name(gpointer instance, const_gchar *detailed_signal,
            void *retvalue)
    void g_error_free(GError *error)
    bool gst_element_query_position(
            GstElement *element, GstFormat format, gint64 *cur) nogil
    bool gst_element_query_duration(
            GstElement *element, GstFormat format, gint64 *cur) nogil
    bool gst_element_seek_simple(
            GstElement *element, GstFormat format,
            GstSeekFlags seek_flags, gint64 seek_pos) nogil
    void gst_message_parse_error(
            GstMessage *message, GError **gerror, char **debug)
    void gst_message_parse_warning(
            GstMessage *message, GError **gerror, char **debug)
    void gst_message_parse_info(
            GstMessage *message, GError **gerror, char **debug)

cdef extern from '_gstplayer.h':
    void g_object_set_void(GstElement *element, char *name, void *value)
    void g_object_set_caps(GstElement *element, char *value)
    void g_object_set_int(GstElement *element, char *name, int value)
    void g_object_set_double(GstElement *element, char *name, double value) nogil
    void g_object_set_bool(GstElement *element, char *name, bool value)
    void g_object_set_str(GstElement *element, char *name, char *value)

    gulong c_element_get_format_location(GstElement *el, char *segment_file_template, void *userdata)

    gulong c_appsink_set_sample_callback(GstElement *appsink,
            appcallback_t callback, void *userdata)
    void c_appsink_pull_preroll(GstElement *appsink,
            appcallback_t callback, void *userdata) nogil
    gulong c_bus_connect_message(GstBus *bus,
            buscallback_t callback, void *userdata)
    void c_signal_disconnect(GstElement *appsink, gulong handler_id)
    void c_glib_iteration(int count)

    char* format_location_callback(char*, char*, int fragment_id)

cdef extern from 'gst/gststructure.h':
    ctypedef void *GstStructure
    char* gst_structure_get_string(GstStructure* structure, char* fieldname);
    char * gst_structure_get_name(GstStructure * structure);

cdef extern from 'gst/gstmessage.h':
    const GstStructure *gst_message_get_structure(GstMessage *message);

cdef void emmit_signal_to_element(void *c_element, char *name):
    cdef GstSample * ret_value = NULL
    g_signal_emit_by_name(c_element, name, ret_value)


cdef list _instances = []

def _on_player_deleted(wk):
    if wk in _instances:
        _instances.remove(wk)

@atexit.register
def gst_exit_clean():
    # XXX don't use a stop() method or anything that change the state of the
    # element without releasing the GIL. Otherwise, we might have a deadlock due
    # to GIL in appsink callback + GIL already locked here.
    for wk in _instances:
        player = wk()
        if player:
            player.unload()


class GstPlayerException(Exception):
    pass


cdef void _on_appsink_sample(
        void *c_player, int width, int height,
        char *data, int datasize) with gil:
    cdef GstPlayer player = <GstPlayer>c_player
    cdef bytes buf = data[:datasize]
    if player.sample_cb:
        player.sample_cb(width, height, buf)


cdef void _on_gstplayer_message(void *c_player, GstMessage *message) with gil:
    cdef GstPlayer player = <GstPlayer>c_player
    cdef GError *err = NULL
    cdef char* filename
    if message.type == GST_MESSAGE_EOS:
        player.got_eos()
    elif message.type == GST_MESSAGE_ERROR:
        gst_message_parse_error(message, &err, NULL)
        player.message_cb('error', err.message)
        g_error_free(err)
    elif message.type == GST_MESSAGE_WARNING:
        gst_message_parse_warning(message, &err, NULL)
        player.message_cb('warning', err.message)
        g_error_free(err)
    elif message.type == GST_MESSAGE_INFO:
        gst_message_parse_info(message, &err, NULL)
        player.message_cb('info', err.message)
        g_error_free(err)
    elif message.type == GST_MESSAGE_ELEMENT:
        player.message_cb('debug', 'GST_MESSAGE_ELEMENT message')
        try:
            if player.recording_options:
                s = gst_message_get_structure(message)
                name = gst_structure_get_name(s)
                name_str = name.decode('utf-8', 'ignore')
                if player.video_recorder_closed:
                    player.message_cb('debug', f'message GST_MESSAGE_ELEMENT message {name_str} for closed video recorder')
                    return
                if name_str not in ['splitmuxsink-fragment-opened', 'splitmuxsink-fragment-closed']:
                    player.message_cb('debug', f'unknown GST_MESSAGE_ELEMENT message {name_str}')
                    return
                filename = gst_structure_get_string(s, "location")
                filename_str = filename.decode('utf-8', 'ignore')
                player.message_cb('info', f'message GST_MESSAGE_ELEMENT message {name_str} for file {filename_str}')
                if name_str == 'splitmuxsink-fragment-closed' and player.video_recorder_closed is False:
                    player.video_recorder_closed = True
                player.recording_options['segment_callback'](name_str, filename_str)
        except Exception as e:
            player.message_cb('error', f'_on_gstplayer_message exception {str(e)}')
    else:
        pass

def _gst_init():
    if gst_is_initialized():
        return True
    cdef int argc = 0
    cdef char **argv = NULL
    cdef GError *error
    if not gst_init_check(&argc, &argv, &error):
        msg = 'Unable to initialize gstreamer: code={} message={}'.format(
                error.code, <bytes>error.message)
        raise GstPlayerException(msg)

def get_gst_version():
    cdef unsigned int major, minor, micro, nano
    gst_version(&major, &minor, &micro, &nano)
    return (major, minor, micro, nano)


def glib_iteration(int loop):
    c_glib_iteration(loop)

cdef class GstPlayer:
    cdef GstElement *pipeline
    cdef GstElement *playbin
    cdef GstElement *appsink
    cdef GstElement *fakesink
    cdef GstElement *recordingmuxer
    cdef GstBus *bus
    cdef object uri, options, sample_cb, eos_cb, _message_cb, recording_options, video_recorder_closed
    cdef gulong hid_sample, hid_message, hid_on_get_format_location

    cdef object __weakref__

    def __cinit__(self, *args, **kwargs):
        self.pipeline = self.playbin = self.appsink = self.fakesink = NULL
        self.bus = NULL
        self.recordingmuxer = NULL
        self.hid_sample = self.hid_message = self.hid_on_get_format_location = 0

    def message_cb(self, severity, message):
        self._message_cb(severity, f"player 0x{id(self):x} - {message}")

    def __init__(self, uri, sample_cb=None, eos_cb=None, message_cb=None, options=None):
        super(GstPlayer, self).__init__()
        options = options or {}
        self.uri = uri
        self.recording_options = options.pop('recording', None)
        self.options = options
        self.sample_cb = sample_cb
        self.eos_cb = eos_cb
        self._message_cb = message_cb
        self.video_recorder_closed = None
        _instances.append(ref(self, _on_player_deleted))

        # ensure gstreamer is init
        _gst_init()

    def stop_video_recording(self, signal_name='split-now'):
        if self.recordingmuxer:
            self.video_recorder_closed = False
            self.send_recordingmuxer_signal(signal_name)
        else:
            self.message_cb('warning', 'No self.recordingmuxer in stop_video_recording')

    def set_property_int_value(self, name, prop_name, value):
        self.message_cb('info', f'Setting property {name}.{prop_name}={value}')
        recording_valve = gst_bin_get_by_name(<GstBin *> self.pipeline, name)
        g_object_set_int(recording_valve, prop_name, value)
        return value

    def send_recordingmuxer_signal(self, signal_name):
        if self.recordingmuxer:
            self.message_cb('info', 'Sending signal {} to self.recordingmuxer'.format(signal_name))
            emmit_signal_to_element(self.recordingmuxer, signal_name)
        else:
            self.message_cb('warning', 'No recordingmuxer for Sending signal {}'.format(signal_name))

    def set_get_segment_name_callback(self, segment_file_template, element_name='recordingmuxer'):
        self.message_cb('debug', 'set_get_segment_name_callback for segment_file_template={}'.format(segment_file_template))
        self.recordingmuxer = gst_bin_get_by_name(<GstBin *> self.pipeline, element_name)

        if self.recordingmuxer:
            self.message_cb('info', 'setting get name call back')
            c_element_get_format_location(self.recordingmuxer, <char*> segment_file_template, <void *>self)
        else:
            self.message_cb('error', 'no element with name ={}'.format(element_name))

    def __dealloc__(self):
        self.unload()

    cdef void got_eos(self):
        if self.eos_cb:
            self.eos_cb()

    def load(self):
        cdef bytes py_uri
        cdef GError *error = NULL

        # if already loaded before, clean everything.
        if self.pipeline != NULL:
            self.unload()

        # create the pipeline
        if 'pipeline' not in self.options:
            self.pipeline = gst_pipeline_new(NULL)
            if self.pipeline == NULL:
                raise GstPlayerException('Unable to create a pipeline')
        else:
            pipeline_opt = self.options['pipeline']
            if isinstance(pipeline_opt, str):
                pipeline_opt = [s.strip() for s in pipeline_opt.split('!')]
            elementtypes = [s.split(None, 1)[0].strip() for s in pipeline_opt if s]
            if 'appsink' not in elementtypes:
                if 'capsfilter' not in elementtypes:
                    pipeline_opt.append('capsfilter caps="video/x-raw, format=RGB"')
                pipeline_opt.append('appsink name="sink"')
            pipeline_str = ' ! '.join(pipeline_opt).format(**self.options)
            py_uri = <bytes>(pipeline_str).encode('utf-8')
            self.message_cb('debug', 'Pipeline: {}'.format(pipeline_str))
            self.pipeline = gst_parse_launch(<char *>py_uri, &error)
            if self.pipeline != NULL:
                self.appsink = gst_bin_get_by_name(<GstBin *>self.pipeline, "sink")
                if self.appsink == NULL:
                    self.message_cb('error', 'Unable to find name "sink" in pipeline')
                    gst_object_unref(self.pipeline)
                    self.pipeline = NULL
                    return
            else:
                self.message_cb('error', 'Unable to parse a pipeline' + (
                        '' if error == NULL else ', code={} message={}'.format(
                                error.code, <bytes>error.message)
                        ))
                return

        self.bus = gst_pipeline_get_bus(<GstPipeline *>self.pipeline)
        if self.bus == NULL:
            raise GstPlayerException('Unable to get the bus from the pipeline')

        gst_bus_enable_sync_message_emission(self.bus)
        if self.eos_cb or self.message_cb:
            self.hid_message = c_bus_connect_message(
                    self.bus, _on_gstplayer_message, <void *>self)

        if self.appsink == NULL:
            # instantiate the playbin
            self.playbin = gst_element_factory_make('playbin', NULL)
            if self.playbin == NULL:
                raise GstPlayerException(
                    'Unable to create a playbin. Consider setting the environment variable '
                    'GST_REGISTRY to a user accesible path, such as ~/registry.bin')

            gst_bin_add(<GstBin *>self.pipeline, self.playbin)

            # instantiate an appsink
            if self.sample_cb:
                self.appsink = gst_element_factory_make('appsink', NULL)
                if self.appsink == NULL:
                    raise GstPlayerException('Unable to create an appsink')

                g_object_set_void(self.playbin, 'video-sink', self.appsink)

            else:
                self.fakesink = gst_element_factory_make('fakesink', NULL)
                if self.fakesink == NULL:
                    raise GstPlayerException('Unable to create a fakesink')

                g_object_set_void(self.playbin, 'video-sink', self.fakesink)

        if self.appsink != NULL:
            g_object_set_caps(self.appsink, 'video/x-raw,format=RGB')
            g_object_set_int(self.appsink, 'max-buffers', 5)
            g_object_set_int(self.appsink, 'drop', 1)
            g_object_set_int(self.appsink, 'sync', 1)
            g_object_set_int(self.appsink, 'qos', 1)

        # configure playbin
        g_object_set_int(self.pipeline, 'async-handling', 1)
        if self.playbin != NULL:
            py_uri = <bytes>self.uri.encode('utf-8')
            g_object_set_void(self.playbin, 'uri', <char *>py_uri)

        # attach the callback
        # NOTE no need to create a weakref here, as we manage to grab/release
        # the reference of self in the set_sample_callback() method.
        if self.sample_cb:
            self.hid_sample = c_appsink_set_sample_callback(
                    self.appsink, _on_appsink_sample, <void *>self)
        if self.recording_options:
            self.set_get_segment_name_callback(self.recording_options['segment_file_template'])
        # get ready!
        with nogil:
            gst_element_set_state(self.pipeline, GST_STATE_READY)

    def play(self):
        if self.pipeline != NULL:
            with nogil:
                gst_element_set_state(self.pipeline, GST_STATE_PLAYING)

    def stop(self):
        if self.pipeline != NULL:
            with nogil:
                gst_element_set_state(self.pipeline, GST_STATE_NULL)
                gst_element_set_state(self.pipeline, GST_STATE_READY)

    def pause(self):
        if self.pipeline != NULL:
            with nogil:
                gst_element_set_state(self.pipeline, GST_STATE_PAUSED)

    def unload(self):
        cdef GstState current_state, pending_state

        if self.appsink != NULL and self.hid_sample != 0:
            c_signal_disconnect(self.appsink, self.hid_sample)
            self.hid_sample = 0

        if self.bus != NULL and self.hid_message != 0:
            c_signal_disconnect(<GstElement *>self.bus, self.hid_message)
            self.hid_message = 0

        if self.bus != NULL and self.hid_on_get_format_location != 0 and self.recordingmuxer != NULL:
            c_signal_disconnect(<GstElement *>self.recordingmuxer, self.hid_on_get_format_location)
            self.hid_on_get_format_location = 0

        if self.pipeline != NULL:
            # the state changes are async. if we want to guarantee that the
            # state is set to NULL, we need to query it. We also put a 5s
            # timeout for safety, but normally, nobody should hit it.
            with nogil:
                gst_element_set_state(self.pipeline, GST_STATE_NULL)
                gst_element_get_state(self.pipeline, &current_state,
                        &pending_state, <GstClockTime>5e9)
            gst_object_unref(self.pipeline)

        if self.bus != NULL:
            gst_object_unref(self.bus)

        self.appsink = NULL
        self.bus = NULL
        self.pipeline = NULL
        self.playbin = NULL
        self.fakesink = NULL

    def set_volume(self, float volume):
        if self.playbin != NULL:
            # XXX we need to release the GIL, on linux, you might have a race
            # condition. When running, if pulseaudio is used, it might sent a
            # message when you set the volume, in the pulse audio thread
            # The message is received by our common sync-message, and try to get
            # the GIL, and block, because here we didn't release it.
            # 1. our thread get the GIL and ask pulseaudio to set the volume
            # 2. the pulseaudio thread try to sent a message, and wait for the
            #    GIL
            with nogil:
                g_object_set_double(self.playbin, 'volume', volume)

    def get_duration(self):
        cdef double duration
        with nogil:
            duration = <double>self._get_duration()
        if duration == -1:
            return -1
        return duration / float(GST_SECOND)

    def get_position(self):
        cdef double position
        with nogil:
            position = <double>self._get_position()
        if position == -1:
            return -1
        return position / float(GST_SECOND)

    def seek(self, float percent):
        with nogil:
            self._seek(percent)

    #
    # C-like API, that doesn't require the GIL
    #

    cdef gint64 _get_duration(self) nogil:
        cdef gint64 duration = -1
        cdef GstState state
        if self.playbin == NULL:
            return -1

        # check the state
        gst_element_get_state(self.pipeline, &state, NULL,
                <GstClockTime>GST_SECOND)

        # if we are already prerolled, we can read the duration
        if state == GST_STATE_PLAYING or state == GST_STATE_PAUSED:
            gst_element_query_duration(self.playbin, GST_FORMAT_TIME, &duration)
            return duration

        # preroll
        gst_element_set_state(self.pipeline, GST_STATE_PAUSED)
        gst_element_get_state(self.pipeline, &state, NULL,
                <GstClockTime>GST_SECOND)
        gst_element_query_duration(self.playbin, GST_FORMAT_TIME, &duration)
        gst_element_set_state(self.pipeline, GST_STATE_READY)
        return duration

    cdef gint64 _get_position(self) nogil:
        cdef gint64 position = 0
        if self.playbin == NULL:
            return 0
        if not gst_element_query_position(
                self.playbin, GST_FORMAT_TIME, &position):
            return 0
        return position

    cdef void _seek(self, float percent) nogil:
        cdef GstState current_state, pending_state
        cdef gboolean ret
        cdef gint64 seek_t, duration
        if self.playbin == NULL:
            return

        duration = self._get_duration()
        if duration <= 0:
            seek_t = 0
        else:
            seek_t = <gint64>(percent * duration)
        seek_flags = GST_SEEK_FLAG_FLUSH | GST_SEEK_FLAG_KEY_UNIT
        gst_element_get_state(self.pipeline, &current_state,
                &pending_state, <GstClockTime>GST_SECOND)
        if current_state == GST_STATE_READY:
            gst_element_set_state(self.pipeline, GST_STATE_PAUSED)
        ret = gst_element_seek_simple(self.playbin, GST_FORMAT_TIME,
                <GstSeekFlags>seek_flags, seek_t)

        if not ret:
            return

        if self.appsink != NULL:
            gst_element_get_state(self.pipeline, &current_state,
                    &pending_state, <GstClockTime>GST_SECOND)
            if current_state != GST_STATE_PLAYING:
                c_appsink_pull_preroll(
                    self.appsink, _on_appsink_sample, <void *>self)
