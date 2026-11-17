workspace "Parquetry" "Architecture documentation for Parquetry, a Parquet and GeoParquet reader and writer for the JVM" {

    !identifiers flat
    # The relationships below are declared by hand. The container view draws the
    # Maven dependencies only, not the edges implied by the components.
    !impliedRelationships false

    model {
        developer = person "Java developer" "Builds an application or a GeoTools-based service that reads or writes Parquet and GeoParquet" {
            tags "Person"
        }

        operator = person "GeoServer operator" "Publishes GeoParquet files as map and feature layers" {
            tags "Person"
        }

        geoserver = softwareSystem "GeoServer and GeoServer Cloud" "Map and feature servers. The Parquetry plugin of GeoServer and the Parquetry extension of GeoServer Cloud include parquetry-geotools." {
            tags "External"
        }

        tileverseStorage = softwareSystem "tileverse-storage" "Java library for byte-range reads, file listings and uploads on local files, HTTP servers and cloud object stores" {
            tags "External"
        }

        objectStorage = softwareSystem "Object storage" "Local disks, HTTP servers, Amazon S3, Azure Blob Storage and Google Cloud Storage" {
            tags "External"
        }

        arrowConsumers = softwareSystem "Arrow consumers" "Analytics engines reading Apache Arrow, such as DuckDB, Polars, DataFusion and pandas" {
            tags "External"
        }

        parquetry = softwareSystem "Parquetry" "Reads and writes Parquet and GeoParquet with bounded memory, without Hadoop or parquet-java" {
            tags "ParquetrySystem"

            parquetryIo = container "parquetry-io" "Reads byte ranges from files and from tileverse-storage, lists files, and pools the native buffers of reads" "Java library"
            parquetryCompression = container "parquetry-compression" "Compresses and decompresses Parquet pages and Avro blocks" "Java library"
            parquetryFormat = container "parquetry-format" "Decodes and encodes the Parquet footer and page headers. Models the schema and the GeoParquet metadata." "Java library"

            parquetryCore = container "parquetry-core" "Reads and writes single Parquet and GeoParquet files, filters rows, and decodes geometries to JTS" "Java library" {

                group "Read" {
                    parquetFileReader = component "ParquetFileReader" "Opens one file and decodes its footer once. Serves read, readBatches, count, bounds and explain." "Java class"
                    readPlanner = component "ReadPlanner" "Plans a query: runs the filter pipeline over the row groups of the file" "Java class"
                    readPlan = component "ReadPlan" "Holds the surviving row groups of one query and builds their decode, with a row mask for each narrowed row group" "Java class"
                }

                group "Filter pipeline" {
                    filterPipeline = component "FilterPipeline" "Normalizes the predicate, then runs the tiers on the metadata of each row group, cheapest first. Produces an ExplainPlan." "Java class"
                    statsEvaluator = component "StatsEvaluator" "STATS tier: the minimum and maximum of each column chunk, from the footer" "Java class"
                    spatialBoundsEvaluator = component "SpatialBoundsEvaluator" "SPATIAL tier: the GeoParquet 2.0 geometry bounding box of each row group, from the footer" "Java class"
                    columnIndexEvaluator = component "ColumnIndexEvaluator" "COLUMN_INDEX tier: the minimum and maximum of each page, from the page index. Narrows a row group to its surviving rows." "Java class"
                    bloomFilterEvaluator = component "BloomFilterEvaluator" "BLOOM_FILTER tier: probes the bloom filters with the values of Eq and In predicates" "Java class"
                }

                group "Fetch" {
                    rowGroupPrefetcher = component "RowGroupPrefetcher" "Fetches the next row groups ahead of the consumer on virtual threads, within the fetch budget" "Java class"
                    rowGroupFetcher = component "RowGroupFetcher" "Turns the surviving column chunks of a row group into byte ranges, and reads them in one batch call into pooled buffers" "Java class"
                }

                group "Decode" {
                    parallelDecodeCoordinator = component "ParallelDecodeCoordinator" "Decodes several row groups at once on a shared CPU pool, within the decode budget, and returns them in file order" "Java class"
                    maskedScanRowGroupReader = component "MaskedScanRowGroupReader" "Decodes a filtered row group in one pass: the filter columns first, then the output columns of the surviving rows only" "Java class"
                    batchRowGroupReader = component "BatchRowGroupReader" "Decodes a row group without a predicate: the row groups of an unfiltered read, and the MATCHED ones" "Java class"
                    vectorizedPredicateEvaluator = component "VectorizedPredicateEvaluator" "RECORD_LEVEL tier: evaluates the predicate over a window of decoded values" "Java class"
                }

                group "Materialize" {
                    batchPipeline = component "BatchPipeline" "Turns the decoded row groups into a lazy stream of batches or of records" "Java class"
                    materializer = component "Materializer" "Builds the output object of each row, in the record type chosen by the caller" "Java interface"
                }

                group "Write" {
                    parquetFileWriter = component "ParquetFileWriter" "Writes records or batches as row groups, then the page index, the bloom filters, the GeoParquet metadata and the footer" "Java class"
                }
            }

            parquetryCatalog = container "parquetry-catalog" "Reads a directory or a glob of Parquet files as one dataset" "Java library" {
                datasetCatalog = component "DatasetCatalog" "The entry point to the datasets behind one connection, found by name" "Java interface"
                filesetCatalog = component "FilesetCatalog" "Resolves datasets from a listing of files: one dataset merging the files, or one dataset per file" "Java class"
                concurrentFooterGather = component "ConcurrentFooterGather" "Reads the footers of the listed files on virtual threads and keeps their schema, statistics and GeoParquet metadata" "Java class"
                hivePartitionResolver = component "HivePartitionResolver" "Parses the key=value directories of a file path into partition values" "Java class"
                parquetDataset = component "ParquetDataset" "A dataset read as one: read, readBatches, count, bounds and explain over its files" "Java interface"
                filesetDataset = component "FilesetDataset" "A dataset of one or many files sharing a schema. Prunes the files before each query, then reads the surviving ones." "Java class"
                concurrentSurvivorReads = component "ConcurrentSurvivorReads" "Reads several surviving files at once and merges their batches into one stream" "Java class"
                parquetSource = component "ParquetSource" "The read facade over one Parquet file" "Java interface"
            }

            parquetryGeotools = container "parquetry-geotools" "Serves GeoParquet files as GeoTools features, through a GeoTools data store" "Java library" {
                geoParquetDataStoreFactory = component "GeoParquetDataStoreFactory" "Opens the Parquet store from the geoparquet parameter: lists the files and opens a catalog over them" "Java class"
                storageParams = component "StorageParams" "Lists the storage connection parameters of the tileverse-storage backends found on the classpath" "Java class"
                catalogDataStore = component "CatalogDataStore" "Read-only data store over a DatasetCatalog, with one feature type per dataset. GeoParquetDataStore extends it." "Java class"
                featureTypeMapper = component "FeatureTypeMapper" "Maps the Parquet schema of a dataset to a GeoTools feature type" "Java class"
                catalogFeatureSource = component "CatalogFeatureSource" "Serves the features, the count and the bounds of one dataset. Pushes the filter, the attributes and the feature limit into the read." "Java class"
                queryTranslator = component "QueryTranslator" "Splits a GeoTools query into a predicate, the columns to decode and the filter left to GeoTools" "Java class"
                filterToPredicate = component "FilterToPredicate" "Translates GeoTools filters into a parquetry predicate. Exact geometry tests use JtsGeometryFilter." "Java class"
                screenMapReadProbe = component "ScreenMapReadProbe" "Hands the pixels already drawn on a map to the read. The read skips the parts of the files falling on them." "Java class"
                datasetFeatureReader = component "DatasetFeatureReader" "Streams the rows of a dataset read and builds one feature per row" "Java class"
                geoParquetExporter = component "GeoParquetExporter" "Writes a GeoTools feature collection as a GeoParquet file" "Java class"
            }

            parquetryArrow = container "parquetry-arrow" "Exports a read as an Arrow IPC stream or through the Arrow C Data Interface" "Java library"
            parquetryIceberg = container "parquetry-iceberg" "Reads Apache Iceberg tables as datasets" "Java library" {
                tags "Experimental"
            }
            parquetryStac = container "parquetry-stac" "Reads the GeoParquet files of a STAC collection as one dataset" "Java library" {
                tags "Experimental"
            }
            parquetryAvro = container "parquetry-avro" "Reads and writes Avro container files, the format of Iceberg manifests and manifest lists" "Java library"
            parquetryJackson = container "parquetry-jackson" "Renders the rows of a read as JSON" "Java library"
            par = container "par" "Inspects, filters and copies files from the command line" "Command line application" {
                tags "Experimental"
            }
        }

        # System context
        developer -> parquetry "Reads and writes Parquet and GeoParquet with"
        operator -> geoserver "Publishes GeoParquet layers with"
        geoserver -> parquetry "Reads GeoParquet layers through the data store of"
        parquetry -> tileverseStorage "Reads byte ranges, lists files and writes files through"
        parquetry -> arrowConsumers "Hands the rows of a read as Arrow to"
        tileverseStorage -> objectStorage "Reads and writes objects in"

        # Container dependencies, as declared in the module POMs
        parquetryIo -> tileverseStorage "Reads byte ranges and lists files through"
        parquetryFormat -> parquetryIo "Reads footers from byte sources of"
        parquetryCore -> parquetryIo "Reads byte ranges through"
        parquetryCore -> parquetryCompression "Compresses and decompresses pages with"
        parquetryCore -> parquetryFormat "Decodes and encodes footers with"
        parquetryCore -> tileverseStorage "Sends batched range requests to"
        parquetryCatalog -> parquetryCore "Reads the files with"
        parquetryCatalog -> parquetryIo "Lists the files of a dataset with"
        parquetryAvro -> parquetryIo "Reads byte ranges through"
        parquetryAvro -> parquetryCompression "Decompresses Avro blocks with"
        parquetryIceberg -> parquetryCore "Reads data files and prunes them with"
        parquetryIceberg -> parquetryCatalog "Implements the dataset catalog of"
        parquetryIceberg -> parquetryAvro "Reads the Iceberg manifests with"
        parquetryIceberg -> tileverseStorage "Reads table metadata through"
        parquetryStac -> parquetryCore "Reads and prunes files with"
        parquetryStac -> parquetryCatalog "Implements the dataset catalog of"
        parquetryStac -> tileverseStorage "Reads STAC documents and assets through"
        parquetryGeotools -> parquetryCore "Builds predicates with"
        parquetryGeotools -> parquetryCatalog "Reads datasets through"
        parquetryGeotools -> parquetryStac "Opens STAC collections through"
        parquetryGeotools -> parquetryIceberg "Opens Iceberg tables through"
        parquetryGeotools -> tileverseStorage "Declares the storage parameters of"
        parquetryArrow -> parquetryCore "Exports the batches of"
        parquetryJackson -> parquetryCore "Renders the records of"
        par -> parquetryCore "Reads and writes files with"
        par -> parquetryCatalog "Opens datasets with"
        par -> parquetryIceberg "Opens Iceberg tables with"
        par -> parquetryArrow "Writes Arrow output with"
        par -> parquetryJackson "Prints JSON with"
        par -> tileverseStorage "Bundles the storage backends of"

        # Users of the containers
        developer -> parquetryCore "Reads and writes files with"
        developer -> parquetryCatalog "Reads multi-file datasets with"
        developer -> par "Inspects and copies files with"
        geoserver -> parquetryGeotools "Reads GeoParquet layers through"
        parquetryArrow -> arrowConsumers "Hands Arrow IPC streams and Arrow C Data to"

        # parquetry-core components
        developer -> parquetFileReader "Opens files and reads them with"
        developer -> parquetFileWriter "Writes files with"
        parquetryCatalog -> parquetFileReader "Reads each file of a dataset with"
        parquetFileReader -> parquetryFormat "Decodes the footer with"
        parquetFileReader -> readPlanner "Plans queries with"
        parquetFileReader -> readPlan "Starts the decode with"
        parquetFileReader -> batchPipeline "Streams batches and records through"
        readPlanner -> filterPipeline "Prunes row groups and pages with"
        readPlanner -> readPlan "Returns"
        filterPipeline -> statsEvaluator "Runs"
        filterPipeline -> spatialBoundsEvaluator "Runs"
        filterPipeline -> columnIndexEvaluator "Runs"
        filterPipeline -> bloomFilterEvaluator "Runs"
        readPlan -> parallelDecodeCoordinator "Builds"
        batchPipeline -> parallelDecodeCoordinator "Pulls decoded row groups from"
        batchPipeline -> materializer "Builds records with"
        parallelDecodeCoordinator -> rowGroupPrefetcher "Takes fetched row groups from"
        rowGroupPrefetcher -> rowGroupFetcher "Fetches row groups with"
        rowGroupFetcher -> parquetryIo "Reads byte ranges through"
        parallelDecodeCoordinator -> maskedScanRowGroupReader "Decodes filtered row groups with"
        parallelDecodeCoordinator -> batchRowGroupReader "Decodes the other row groups with"
        maskedScanRowGroupReader -> vectorizedPredicateEvaluator "Evaluates the predicate with"
        maskedScanRowGroupReader -> parquetryCompression "Decompresses pages with"
        batchRowGroupReader -> parquetryCompression "Decompresses pages with"
        parquetFileWriter -> parquetryFormat "Encodes the footer and page headers with"
        parquetFileWriter -> parquetryCompression "Compresses pages with"
        parquetFileWriter -> parquetryIo "Writes to a ByteSink of"

        # parquetry-catalog components
        developer -> filesetCatalog "Opens datasets with"
        parquetryGeotools -> datasetCatalog "Reads datasets through"
        parquetryIceberg -> datasetCatalog "Implements"
        parquetryStac -> datasetCatalog "Implements"
        filesetCatalog -> datasetCatalog "Implements"
        filesetCatalog -> parquetryIo "Lists the files with a FileSource of"
        filesetCatalog -> concurrentFooterGather "Gathers the metadata of the files with"
        filesetCatalog -> hivePartitionResolver "Reads partition values with"
        filesetCatalog -> filesetDataset "Builds on first access"
        datasetCatalog -> parquetDataset "Returns"
        filesetDataset -> parquetDataset "Implements"
        filesetDataset -> parquetryCore "Prunes files with FilePruner of"
        filesetDataset -> concurrentSurvivorReads "Merges the reads of the surviving files with"
        filesetDataset -> parquetSource "Opens each surviving file as"
        concurrentFooterGather -> parquetSource "Opens each file as"
        parquetSource -> parquetryCore "Reads one file with ParquetFileReader of"

        # parquetry-geotools components
        geoserver -> geoParquetDataStoreFactory "Opens the store with"
        geoserver -> catalogFeatureSource "Queries a layer with"
        geoserver -> datasetFeatureReader "Iterates over the features of"
        geoserver -> geoParquetExporter "Writes the geoparquet WFS output format with"
        geoParquetDataStoreFactory -> storageParams "Declares the storage parameters with"
        geoParquetDataStoreFactory -> parquetryIo "Lists the files with a FileSource of"
        geoParquetDataStoreFactory -> parquetryCatalog "Opens a FilesetCatalog of"
        geoParquetDataStoreFactory -> catalogDataStore "Creates"
        storageParams -> tileverseStorage "Finds the storage backends of"
        catalogDataStore -> parquetryCatalog "Resolves datasets with a DatasetCatalog of"
        catalogDataStore -> catalogFeatureSource "Creates one per feature type"
        catalogFeatureSource -> featureTypeMapper "Builds the feature type with"
        catalogFeatureSource -> queryTranslator "Translates queries with"
        catalogFeatureSource -> screenMapReadProbe "Hands the drawn pixels of a map to the read with"
        catalogFeatureSource -> datasetFeatureReader "Reads features with"
        catalogFeatureSource -> parquetryCatalog "Counts and bounds features with a ParquetDataset of"
        queryTranslator -> filterToPredicate "Translates the pushable filter with"
        filterToPredicate -> parquetryCore "Builds predicates and geometry filters of"
        datasetFeatureReader -> parquetryCatalog "Reads rows from a ParquetDataset of"
        geoParquetExporter -> parquetryCore "Writes with ParquetFileWriter of"
    }

    views {
        properties {
            "c4plantuml.tags" "true"
        }

        systemContext parquetry "SystemContext" {
            include *
            include operator
            include objectStorage
            autoLayout
            title "System Context - Parquetry"
            description "Parquetry between the applications using it and the storage holding the files."
        }

        container parquetry "Containers" {
            include *
            # parquetry-io holds the adapter to tileverse-storage. The other edges
            # to tileverse-storage are left out to keep the module graph readable.
            exclude "parquetryCore -> tileverseStorage"
            exclude "parquetryIceberg -> tileverseStorage"
            exclude "parquetryStac -> tileverseStorage"
            exclude "parquetryGeotools -> tileverseStorage"
            exclude "par -> tileverseStorage"
            exclude "developer -> parquetryCatalog"
            autoLayout tb
            title "Containers - the Parquetry modules"
            description "The modules of Parquetry and the Maven dependencies between them."
        }

        component parquetryCore "CoreComponents" {
            include *
            exclude "developer -> parquetryCatalog"
            exclude "parquetryCatalog -> parquetryIo"
            exclude "parquetryFormat -> parquetryIo"
            autoLayout tb
            title "Components - parquetry-core"
            description "The read pipeline and the writer of one Parquet file."
        }

        component parquetryCatalog "CatalogComponents" {
            include *
            exclude "developer -> parquetryCore"
            exclude "parquetryCore -> parquetryIo"
            exclude "parquetryGeotools -> parquetryCore"
            exclude "parquetryGeotools -> parquetryStac"
            exclude "parquetryGeotools -> parquetryIceberg"
            exclude "parquetryStac -> parquetryCore"
            exclude "parquetryIceberg -> parquetryCore"
            autoLayout lr
            title "Components - parquetry-catalog"
            description "The catalogs and datasets reading many Parquet files as one."
        }

        component parquetryGeotools "GeoToolsComponents" {
            include *
            exclude "parquetryCatalog -> parquetryIo"
            exclude "parquetryCatalog -> parquetryCore"
            exclude "parquetryCore -> parquetryIo"
            exclude "parquetryCore -> tileverseStorage"
            autoLayout lr
            title "Components - parquetry-geotools"
            description "The GeoTools data store reading GeoParquet datasets."
        }

        styles {
            element "Person" {
                shape Person
                background #613d29
                color #f7f4f4
            }
            element "ParquetrySystem" {
                background #613d29
                color #f7f4f4
            }
            element "Container" {
                background #613d29
                color #f7f4f4
            }
            element "Component" {
                background #613d29
                color #f7f4f4
            }
            element "Experimental" {
                border dashed
                background #8c6a55
                color #ffffff
            }
            element "External" {
                background #8a8a86
                color #ffffff
            }
            element "Group" {
                color #613d29
            }
            relationship "Relationship" {
                style solid
            }
        }
    }
}
