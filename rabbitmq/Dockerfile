FROM rabbitmq:4.0.9-management

# Instalar el plugin rabbitmq_delayed_message_exchange
# Versión compatible con RabbitMQ 4.x
# Releases: https://github.com/rabbitmq/rabbitmq-delayed-message-exchange/releases
ARG DELAYED_PLUGIN_VERSION=4.0.7

RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && curl -fsSL \
       "https://github.com/rabbitmq/rabbitmq-delayed-message-exchange/releases/download/v${DELAYED_PLUGIN_VERSION}/rabbitmq_delayed_message_exchange-v${DELAYED_PLUGIN_VERSION}.ez" \
       -o "/opt/rabbitmq/plugins/rabbitmq_delayed_message_exchange-v${DELAYED_PLUGIN_VERSION}.ez" \
    && apt-get purge -y curl && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

COPY enabled_plugins /etc/rabbitmq/enabled_plugins
