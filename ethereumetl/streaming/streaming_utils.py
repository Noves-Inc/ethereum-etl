from ethereumetl.jobs.exporters.console_item_exporter import ConsoleItemExporter


def get_item_exporter(output):
    if output is not None:
        from ethereumetl.jobs.exporters.google_pubsub_item_exporter import GooglePubSubItemExporter
        item_exporter = GooglePubSubItemExporter(item_type_to_topic_mapping={
            'block': output + '.blocks',
            'transaction': output + '.transactions',
            'log': output + '.logs',
            'token_transfer': output + '.token_transfers',
            'trace': output + '.traces',
            'contract': output + '.contracts',
            'token': output + '.tokens',
        })
    else:
        item_exporter = ConsoleItemExporter()

    return item_exporter
