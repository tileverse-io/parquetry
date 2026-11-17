workspace extends workspace.dsl {

    name "Parquetry - dynamic views"
    description "Runtime scenarios of Parquetry: a filtered read of one file, and a GeoServer request reaching a GeoParquet file"

    views {
        dynamic parquetryCore "FilteredRead" {
            title "Dynamic view - a filtered read of one file"
            description "Plan and prune on the metadata, then fetch and decode the surviving pages as the stream is pulled."

            developer -> parquetFileReader "read(predicate, projection, options)"
            parquetFileReader -> readPlanner "plan the query"
            readPlanner -> filterPipeline "evaluate each row group, cheapest tier first"
            filterPipeline -> statsEvaluator "STATS: drop the row groups ruled out by min and max"
            filterPipeline -> spatialBoundsEvaluator "SPATIAL: drop the row groups outside the bounding box"
            filterPipeline -> columnIndexEvaluator "COLUMN_INDEX: narrow the row groups left to their surviving pages"
            filterPipeline -> bloomFilterEvaluator "BLOOM_FILTER: drop the row groups lacking an Eq or In value"
            readPlanner -> readPlan "return the outcome of each row group"
            parquetFileReader -> readPlan "decode(survivors, shape)"
            readPlan -> parallelDecodeCoordinator "build the row masks and the decode"
            parquetFileReader -> batchPipeline "return a lazy stream; no column data is fetched yet"
            batchPipeline -> parallelDecodeCoordinator "on the first pull: next row group, in file order"
            parallelDecodeCoordinator -> rowGroupPrefetcher "take the fetched bytes of the row group"
            rowGroupPrefetcher -> rowGroupFetcher "fetch the row group, and the next ones ahead"
            rowGroupFetcher -> parquetryIo "read the byte ranges of the surviving pages"
            parquetryIo -> tileverseStorage "readRanges"
            tileverseStorage -> objectStorage "range requests"
            parallelDecodeCoordinator -> maskedScanRowGroupReader "decode the row group in one masked pass"
            maskedScanRowGroupReader -> vectorizedPredicateEvaluator "RECORD_LEVEL: evaluate the predicate over each window"
            batchPipeline -> materializer "build a record from each surviving row"

            autoLayout tb
        }

        dynamic parquetryGeotools "GeoServerRequest" {
            title "Dynamic view - a GeoServer request reaching a GeoParquet file"
            description "A WMS GetMap or a WFS GetFeature request served by the Parquet store from object storage."

            geoserver -> catalogFeatureSource "getFeatures(query) for a WMS or WFS request"
            catalogFeatureSource -> queryTranslator "split the query"
            queryTranslator -> filterToPredicate "translate the pushable filter into a predicate"
            catalogFeatureSource -> screenMapReadProbe "for a map: hand the drawn pixels to the read"
            catalogFeatureSource -> datasetFeatureReader "open a reader with the predicate and the columns"
            datasetFeatureReader -> parquetryCatalog "ParquetDataset.read(predicate, projection, options)"
            parquetryCatalog -> parquetryCore "prune the files, then read each surviving file"
            parquetryCore -> parquetryIo "read the byte ranges left after filtering"
            parquetryIo -> tileverseStorage "readRanges"
            tileverseStorage -> objectStorage "range requests"
            geoserver -> datasetFeatureReader "next(): one feature per matching row"

            autoLayout tb
        }
    }
}
