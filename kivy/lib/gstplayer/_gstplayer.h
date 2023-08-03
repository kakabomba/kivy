#include <glib.h>
#include <gst/gst.h>
#include <stdbool.h>
#include <time.h>
#include <sys/time.h>
#include <stdio.h>

static void c_glib_iteration(int count)
{
	while (count > 0 && g_main_context_pending(NULL))
	{
		count --;
		g_main_context_iteration(NULL, FALSE);
	}
}

static void g_object_set_void(GstElement *element, char *name, void *value)
{
	g_object_set(G_OBJECT(element), name, value, NULL);
}

static void g_object_set_double(GstElement *element, char *name, double value)
{
	g_object_set(G_OBJECT(element), name, value, NULL);
}

static void g_object_set_bool(GstElement *element, char *name, bool value)
{
	g_object_set(G_OBJECT(element), name, value, NULL);
}

static void g_object_set_obj(GstElement *element, char *name, GstElement *value)
{
	g_object_set(G_OBJECT(element), name, value, NULL);
}
static void g_object_set_str(GstElement *element, char *name, char *value)
{
	g_object_set(G_OBJECT(element), name, value, NULL);
}

static void g_object_set_caps(GstElement *element, char *value)
{
	GstCaps *caps = gst_caps_from_string(value);
	g_object_set(G_OBJECT(element), "caps", caps, NULL);
}

static void g_object_set_int(GstElement *element, char *name, int value)
{
	g_object_set(G_OBJECT(element), name, value, NULL);
}

typedef void (*appcallback_t)(void *, int, int, char *, int);
typedef void (*buscallback_t)(void *, GstMessage *);
typedef struct {
	appcallback_t callback;
	buscallback_t bcallback;
	char eventname[15];
	PyObject *userdata;
} callback_data_t;

typedef struct {
	char *segment_file_template;
} callback_get_format_location_data_t;

static GstFlowReturn c_on_appsink_sample(GstElement *appsink, callback_data_t *data)
{
	GstSample *sample = NULL;
	GstBuffer *buffer = NULL;
	GstMapInfo mapinfo;
	GstCaps *caps = NULL;
	GstStructure *structure = NULL;
	gchar *cbuffer = NULL, *dstbuffer, *srcbuffer;
	gint width4, width3, y;
	gint width, height, size;

	g_signal_emit_by_name (appsink, data->eventname, &sample);
	if ( sample == NULL ) {
		g_warning("Could not get sample");
		goto done;
	}

	caps = gst_sample_get_caps(sample);
	if ( caps == NULL ) {
		g_warning("Could not get snapshot format");
		goto done;
	}

	structure = gst_caps_get_structure(caps, 0);
	gst_structure_get_int(structure, "width", &width);
	gst_structure_get_int(structure, "height", &height);

	buffer = gst_sample_get_buffer(sample);

	if ( gst_buffer_map(buffer, &mapinfo, GST_MAP_READ) != TRUE ) {
		g_debug("Unable to map buffer");
		goto done;
	}

	width3 = width * 3;
	width4 = GST_ROUND_UP_4(width3);
	if ( width4 == width3 ) {
		// we can directly use the buffer in memory
		cbuffer = (gchar *)mapinfo.data;
		size = (gint)mapinfo.size;
	} else {
		// need a copy without stride :(
		// OpenGL ES 2 doesn't support stride without an extension.  We might
		// pass the stride information into the callback, and then ask
		// texture.blit_buffer(..., stride=width4), in order to let desktop or
		// mobile with extension to copy the row width stride.
		// NVIDIA extension:
		// http://www.khronos.org/registry/gles/extensions/EXT/GL_EXT_unpack_subimage.txt
		size = width * height * 3;
		dstbuffer = cbuffer = (gchar *)g_malloc(size);
		if ( cbuffer == NULL ) {
			g_warning("Unable to create destination buffer");
			goto done;
		}
		srcbuffer = (gchar *)mapinfo.data;
		for ( y = 0; y < height; y++ ) {
			memcpy(dstbuffer, srcbuffer, width3);
			dstbuffer += width3;
			srcbuffer += width4;
		}
	}

	data->callback(data->userdata, width, height, (char *)cbuffer, size);

	if ( width4 != width3 )
		g_free(cbuffer);

	gst_buffer_unmap(buffer, &mapinfo);

done:
	//gst_caps_unref(caps);
	if ( sample != NULL )
		gst_sample_unref(sample);
	return GST_FLOW_OK;
}

static void c_signal_free_data(gpointer data, GClosure *closure)
{
	callback_data_t *cdata = data;
	Py_DECREF(cdata->userdata);
	free(cdata);
}

static gulong c_appsink_set_sample_callback(GstElement *appsink, appcallback_t callback, PyObject *userdata)
{
	callback_data_t *data = (callback_data_t *)malloc(sizeof(callback_data_t));
	if ( data == NULL )
		return 0;
	data->callback = callback;
	data->bcallback = NULL;
	data->userdata = userdata;
	strcpy(data->eventname, "pull-sample");

	Py_INCREF(data->userdata);

    g_object_set(G_OBJECT(appsink), "emit-signals", TRUE, NULL);

	return g_signal_connect_data(
			appsink, "new-sample",
			G_CALLBACK(c_on_appsink_sample), data,
			c_signal_free_data, 0);
}

static void c_appsink_pull_preroll(GstElement *appsink, appcallback_t callback, PyObject *userdata)
{
	callback_data_t data;
	data.callback = callback;
	data.userdata = userdata;
	strcpy(data.eventname, "pull-preroll");
	c_on_appsink_sample(appsink, &data);
}

static void c_signal_disconnect(GstElement *element, gulong handler_id)
{
	g_signal_handler_disconnect(element, handler_id);
}

static gboolean c_on_bus_message(GstBus *bus, GstMessage *message, callback_data_t *data)
{
	//g_return_val_if_fail( GST_MESSAGE_TYPE( message ) == GST_MESSAGE_EOS, FALSE);
	data->bcallback(data->userdata, message);
	return TRUE;
}

static gulong c_bus_connect_message(GstBus *bus, buscallback_t callback, PyObject *userdata)
{
	callback_data_t *data = (callback_data_t *)malloc(sizeof(callback_data_t));
	if ( data == NULL )
		return 0;
	data->callback = NULL;
	data->bcallback = callback;
	data->userdata = userdata;

	Py_INCREF(data->userdata);

	return g_signal_connect_data(
			(GstElement *)bus, "sync-message",
			G_CALLBACK(c_on_bus_message), data,
			c_signal_free_data, 0);
}

static void c_signal_free_get_format_location_data_t(gpointer data, GClosure *closure)
{
	callback_get_format_location_data_t *cdata = data;
//	Py_DECREF(cdata);
	free(cdata);
}

static char* format_location_callback(const gchar *segment_file_template, int fragment_id)
{
    g_info("format_location_callback");
    time_t timer;
    struct tm* tm_info;
    struct timeval tv;
    char date_time_buffer[26];
    char date_time_buffer_usec[33];
    char* ret = (char *)malloc(200 * sizeof(char));
    int usec;

    timer = time(NULL);
    tm_info = localtime(&timer);
    strftime(date_time_buffer, 26, "%Y-%m-%d-%H-%M-%S-%06d", tm_info);
    gettimeofday(&tv, NULL);
    usec = tv.tv_usec;
    sprintf(date_time_buffer_usec, "%s-%06d", date_time_buffer, usec);
    sprintf(ret, segment_file_template, date_time_buffer_usec, fragment_id);
    return ret;
}
static char* c_on_get_format_location(GstElement *el, guint fragment_id, callback_get_format_location_data_t *data)
{
	//g_return_val_if_fail( GST_MESSAGE_TYPE( message ) == GST_MESSAGE_EOS, FALSE);
	g_info("c_on_get_format_location");
	return format_location_callback(data->segment_file_template, fragment_id);
}

static gulong c_element_get_format_location(GstElement *el, char *segment_file_template)
{
	callback_get_format_location_data_t *data = (callback_get_format_location_data_t *)malloc(sizeof(callback_get_format_location_data_t));
	if ( data == NULL )
		return 0;
	data->segment_file_template = segment_file_template;
//	Py_INCREF(data);

	return g_signal_connect_data((GstElement *)el, "format-location",
			G_CALLBACK(c_on_get_format_location), data,
			c_signal_free_get_format_location_data_t, 0);
}

