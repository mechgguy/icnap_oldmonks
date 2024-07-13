docker run \
    --rm \
    --network host \
    --name mosquitto-broker \
    -v $PWD/mqtt_config/mosquitto.conf:/mosquitto/config/mosquitto.conf:ro \
    --user "$(id -u):$(id -g)" \
    eclipse-mosquitto