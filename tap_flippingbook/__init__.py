#!/usr/bin/env python3
import os
import json
import singer
import requests
import time
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema


REQUIRED_CONFIG_KEYS = ["token", "baseUrl"]

LOGGER = singer.get_logger()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        # TODO: populate any metadata and stream's key properties here..
        stream_metadata = []
        key_properties = []
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata,
                replication_key=None,
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
                replication_method=None,
            )
        )
    return Catalog(streams)
    
def sync(config, state, catalog):
    """ Sync data from tap source """
    # Loop over selected streams in catalog
    offset :   int = 0
    Ids:       list = []
    endpoint: dict = {
        "publications": r"/publication",
        "sources": r"/publication/{id}/source",
        "trackedLinks": r"/tracked_links",
        "customDomains": r"/custom-domains",
    }
    
    delay : int =  1    
    for stream in catalog.get_selected_streams(state):
        #LOGGER.info("Syncing stream:" + stream.tap_stream_id)

        bookmark_column = stream.replication_key
        is_sorted = True  # TODO: indicate whether data is sorted ascending on bookmark value

        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=stream.schema.to_dict(),
            key_properties=stream.key_properties,
        )
        session = requests.Session()
        authorization : str  = 'Bearer ' + config.get('token')
        header        : dict = {'authorization': authorization}

        EndPoint = [config.get('baseUrl') + endpoint[stream.stream]]
        if stream.stream == 'sources':
            generatedEndPoints : list = []
            for i in range(len(Ids)):
                generatedEndPoint = EndPoint[0].replace('{id}', Ids[i])
                generatedEndPoints.append(generatedEndPoint)
            EndPoint = generatedEndPoints   
                            
        for endPoint in EndPoint:
            streamIsNotEmpty = True
            while streamIsNotEmpty:
                params        : dict = {
                    'count': 100,
                    'offset': offset,
                }
                try:
                    response = session.request("GET", endPoint , headers=header, params=params)
                except Exception as e:
                    LOGGER.error("failed to get data from the end point: "+ endPoint)
                tap_data = response.json()

                if len(tap_data[stream.stream]) > 0 :
                    offset += params['count']
                    time.sleep(delay)
                    
                    max_bookmark = None
                    for row in tap_data[stream.stream]:
                        if stream.stream == 'publications': ## Gather Ids for source endpoint
                            Ids.append(row['id'])

                        singer.write_records(stream.tap_stream_id, [row])
                        if bookmark_column:
                            if is_sorted:
                                # update bookmark to latest value
                                singer.write_state({stream.tap_stream_id: row[bookmark_column]})
                            else:
                                # if data unsorted, save max value until end of writes
                                max_bookmark = max(max_bookmark, row[bookmark_column])
                    if bookmark_column and not is_sorted:
                        singer.write_state({stream.tap_stream_id: max_bookmark})
                else:
                    streamIsNotEmpty = False
                    break

    return


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
            
        ## swap the streams so the building stream becomes the primary one
        for i in range(len(catalog.streams)-1): # -1 skips the last index in case the stream is already at the end 
            if catalog.streams[i].stream == "sources":
                catalog.streams[-1],catalog.streams[i] = catalog.streams[i], catalog.streams[-1]
        try:        
            sync(args.config, args.state, catalog)
            
        except Exception as e:
            LOGGER.error("Fieled to sync the stream"+ e.message + e.args)

if __name__ == "__main__":
    main()
