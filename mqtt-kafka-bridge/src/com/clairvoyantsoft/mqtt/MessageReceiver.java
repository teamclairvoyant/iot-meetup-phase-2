package com.clairvoyantsoft.mqtt;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.eclipse.kura.core.cloud.CloudPayloadProtoBufDecoderImpl;
import org.eclipse.kura.message.KuraPayload;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public class MessageReceiver implements MqttCallback {

    //  final Producer<Long, String> producer = ProducerCreator.createProducer();

    @Override
    public void connectionLost(Throwable cause) {
        // TODO Auto-generated method stub

    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        byte[] payload = message.getPayload();

        String msg = new String(payload);

        if (msg.startsWith("alert")) {
            System.out.println("Alert received " + msg);
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        Process p = null;
                        if (msg.equals("alert_temp")) {
                            p = Runtime.getRuntime().exec("ls -aF");
                        }
                        if (msg.equals("alert_hum")) {
                            p = Runtime.getRuntime().exec("ls -aF");
                        }
                        if (p != null) {
                            p.waitFor();
                            System.out.println("exit value: " + p.exitValue());
                            p.destroy();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }).start();

        } else {

            try {

                //write different parsing logic for different brokers/topics/message

                final CloudPayloadProtoBufDecoderImpl decoder =
                    new CloudPayloadProtoBufDecoderImpl(payload);

                KuraPayload kuraPayload = decoder.buildFromByteArray();

                Object humidityReading = kuraPayload.getMetric("Humit");
                Object temperatureReading = kuraPayload.getMetric("Tempt");
                String humidity = String.valueOf(humidityReading);
                String temperature = String.valueOf(temperatureReading);

                if (Double.valueOf(temperature) > 20d) {
                    new Thread(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                MqttMessage alert = new
                                    MqttMessage("alert_temp".getBytes());
                                message.setQos(0);
                                PahoMqttClient.getInstance().getClient()
                                    .publish("kapua-sys/Meetup_Kapua/DHT11Sensor/alert", alert);
                                System.out.println("sent temp alert");
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }).start();
                }

                if (Double.valueOf(humidity) > 60d) {
                    new Thread(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                MqttMessage alert = new
                                    MqttMessage("alert_hum".getBytes());
                                message.setQos(0);
                                PahoMqttClient.getInstance().getClient()
                                    .publish("kapua-sys/Meetup_Kapua/DHT11Sensor/alert", alert);
                                System.out.println("sent humid alert");
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }).start();

                }

                String recordString =
                    "device_001|" + kuraPayload.getTimestamp().getTime() + "|" + temperature + "|"
                    + humidity;

                System.out.println("recordString " + recordString);

                final ProducerRecord<Long, String> record =
                    new ProducerRecord<>(topic, recordString);
/*
                producer.send(record, (metadata, exception) -> {
                    long elapsedTime =
                        System.currentTimeMillis() - kuraPayload.getTimestamp().getTime();
                    if (metadata != null) {
                        System.out.printf(
                            "sent record(value=%s) " + "meta(partition=%d, offset=%d) time=%d\n",
                            record.value(), metadata.partition(), metadata.offset(), elapsedTime);
                    } else {
                        exception.printStackTrace();
                    }
                });
*/

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        // TODO Auto-generated method stub

    }

}
