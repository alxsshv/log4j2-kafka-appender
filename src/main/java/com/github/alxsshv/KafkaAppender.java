package com.github.alxsshv;

import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.appender.AppenderLoggingException;
import org.apache.logging.log4j.core.config.Node;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.config.plugins.validation.constraints.Required;
import org.apache.logging.log4j.core.layout.JsonLayout;
import org.apache.logging.log4j.core.util.Booleans;

import java.io.Serializable;


@Plugin(name = "KafkaAppender",
        category = Node.CATEGORY)
public class KafkaAppender extends AbstractAppender {

    private final KafkaSender kafkaSender;

    protected KafkaAppender(String name,
                            Filter filter,
                            Layout<? extends Serializable> layout,
                            boolean ignoreExceptions,
                            Property[] properties,
                            String topic,
                            Property[] kafkaProperties) {
        super(name, filter, layout, ignoreExceptions, properties);
        this.kafkaSender = new KafkaSender(topic, kafkaProperties);
    }

    @PluginFactory
    public static KafkaAppender createAppender(
            @PluginAttribute("name")
            String name,

            @PluginAttribute("ignoreExceptions")
            String ignoreExceptionAttribute,

            @PluginAttribute("kafkaTopic")
            @Required(message = "Please set appender attribute kafkaTopic")
            String topic,

            @PluginElement("Filter") Filter filter,

            @PluginElement("Layout") Layout<? extends Serializable> layoutElement,

            @PluginElement("KafkaProperties")
            @Required(message = "please set element KafkaProperties. Requirement property - bootstrap.servers.")
            Property[] kafkaProperties,

            @PluginElement("Properties")
            Property[] properties) {

        boolean ignoreExceptions = Booleans.parseBoolean(ignoreExceptionAttribute, true);
        Layout<? extends Serializable> layout = (layoutElement != null) ? layoutElement : JsonLayout.createDefaultLayout();
        return new KafkaAppender(name, filter, layout, ignoreExceptions, properties, topic, kafkaProperties);
    }

    @Override
    public void append(LogEvent event) {
        try {
            String message = getLayout().toSerializable(event).toString();
            kafkaSender.send(message);
        } catch (Exception ex) {
            throw new AppenderLoggingException("Kafka appender error: %s", ex.getMessage());
        }
    }

    @Override
    public void stop() {
        if (kafkaSender != null) {
            kafkaSender.close();
        }
        super.stop();
    }
}
