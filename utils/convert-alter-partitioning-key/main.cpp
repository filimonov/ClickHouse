#include <DataTypes/DataTypeDate.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/localBackup.h>
#include <Common/Exception.h>

#include <boost/program_options.hpp>
#include <Poco/Path.h>
#include <Poco/File.h>

#include <iostream>
#include <Disks/DiskLocal.h>
#include <Interpreters/Context.h>
#include <Storages/StorageMergeTree.h>
// #include <Poco/Util/MapConfiguration.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int DIRECTORY_ALREADY_EXISTS;
    extern const int BAD_DATA_PART_NAME;
    extern const int NO_FILE_IN_DATA_PART;
}

/** std::shared_ptr<IDisk> disk = std::make_shared<DiskLocal>("local", "/", 0);
    auto old_part_path = Poco::Path::forDirectory(part_path);
    //const String & old_part_name = old_part_path.directory(old_part_path.depth() - 1);
    String old_part_path_str = old_part_path.toString();

    ReadBufferFromFile column_read_buffer(old_part_path_str + "t.bin", 4096);

    const IDataType & type = DataTypeDateTime();
    auto new_column = type.createColumn();

    IDataType::DeserializeBinaryBulkSettings settings;
    settings.getter = [&](IDataType::SubstreamPath) -> ReadBuffer * { return &column_read_buffer; };
    settings.position_independent_encoding = false; */


void run(String part_path, String /* date_column */, String /* dest_path */)
{
    auto old_part_path = Poco::Path::forDirectory(part_path);
    const String & old_part_name = old_part_path.directory(old_part_path.depth() - 1);
    String old_part_path_str = old_part_path.toString();
    String old_part_parent_dir = old_part_path.popDirectory().toString();

    // std::cout << " old_part_path_str " << old_part_path_str << std::endl
    //         << " old_part_name " << old_part_name  << std::endl
    //         << " old_part_parent_dir " << old_part_parent_dir  << std::endl;

    auto shared_context = Context::createShared();
    auto global_context = std::make_unique<Context>(Context::createGlobal(shared_context.get()));
    global_context->makeGlobalContext();
    global_context->setApplicationType(Context::ApplicationType::LOCAL);
    // global_context->setPath("");
    // global_context->setTemporaryStorage("/tmp");
    // global_context->setFlagsPath("/tmp");
    // global_context->setUserFilesPath(""); // user's files are everywhere
    global_context->setMarkCache(41943040);
    global_context->setUncompressedCache(0);

    //Poco::AutoPtr
    // config;

//    MapConfiguration


    Poco::AutoPtr<Poco::Util::AbstractConfiguration> config = new Poco::Util::MapConfiguration();

//    Poco::allocConfiguration();


    global_context->setConfig(config);
    auto metadata = StorageInMemoryMetadata();

    auto merge_tree_data = StorageMergeTree::create(
            StorageID("dummy","dummy",UUIDHelpers::generateV4()),
            old_part_parent_dir, // relative_data_path
            ,
            false, // attach
            global_context->getGlobalContext(),
            "", // date_column_name
            MergeTreeData::MergingParams(),
            std::make_unique<MergeTreeSettings>(global_context->getMergeTreeSettings()),
            false // has_force_restore_data_flag
    );

    std::shared_ptr<IDisk> disk = std::make_shared<DiskLocal>("local", "/", 0);
    auto single_disk_volume = std::make_shared<SingleDiskVolume>("single_disk_local_volume", disk);

    MergeTreePartInfo part_info;

    std::optional<MergeTreeDataFormatVersion> format_version;

    for (auto v : std::vector<MergeTreeDataFormatVersion>(MergeTreeDataFormatVersion(1), MergeTreeDataFormatVersion(0)) )
    {
        if (MergeTreePartInfo::tryParsePartName(old_part_name, &part_info, v))
        {
            format_version = v;
            std::cout << "format_version = " << v << std::endl;
        }
    }

    if (!format_version.has_value())
    {
        throw Exception("Can not parse part name: " + old_part_name,  ErrorCodes::BAD_DATA_PART_NAME);
    }

//     MergeTreeData::MutableDataPartPtr new_data_part = merge_tree_data->createPart();

// void MergeTreeData::loadDataParts(bool skip_sanity_checks)

//             const auto & part_name = part_names_with_disks[i].first;
//             const auto part_disk_ptr = part_names_with_disks[i].second;



//
//             auto part = createPart(part_name, part_info, single_disk_volume, part_name);
// part->loadColumnsChecksumsIndexes(require_part_metadata, true);



    /// Load global settings from default_profile and system_profile.
    /*global_context->setDefaultProfiles(config());

            std::string tmp_path = config().getString("tmp_path", path + "tmp/");
        std::string tmp_policy = config().getString("tmp_policy", "");
        const VolumePtr & volume = global_context->setTemporaryStorage(tmp_path, tmp_policy);
                global_context->setUserFilesPath(user_files_path);

                global_context->updateStorageConfiguration(*config);
    global_context->setMarkCache(mark_cache_size);

MergeTreeData::MergeTreeData(
    const StorageID & table_id_,
    const String & relative_data_path_,
    const StorageInMemoryMetadata & metadata_,
    Context & context_,
    const String & date_column_name,
    const MergingParams & merging_params_,
    std::unique_ptr<MergeTreeSettings> storage_settings_,
    bool require_part_metadata_,
    bool attach,
    BrokenPartCallback broken_part_callback_)


    IMergeTreeDataPart(
        const MergeTreeData & storage_,
        const String & name_,
        const MergeTreePartInfo & info_,
        const VolumePtr & volume,
        const std::optional<String> & relative_path,
        Type part_type_);

loadPartAndFixMetadata


MergeTreeData::MutableDataPartsVector MergeTreeData::tryLoadPartsToAttach(const ASTPtr & partition, bool attach_part,
        const Context & context, PartsTemporaryRename & renamed_parts)
{
    String source_dir = "detached/";



for (auto it = disk->iterateDirectory(relative_data_path + source_dir); it->isValid(); it->next())
{
    const String & name = it->name();
    MergeTreePartInfo part_info;
    // TODO what if name contains "_tryN" suffix?
    /// Parts with prefix in name (e.g. attaching_1_3_3_0, deleting_1_3_3_0) will be ignored
    if (!MergeTreePartInfo::tryParsePartName(name, &part_info, format_version)
        || part_info.partition_id != partition_id)
    {
        continue;
    }
    LOG_DEBUG(log, "Found part {}", name);
    active_parts.add(name);
    name_to_disk[name] = disk;
}




MergeTreeData::MutableDataPartPtr MergeTreeData::createPart(
    const String & name, const MergeTreePartInfo & part_info,
    const VolumePtr & volume, const String & relative_path) const
{
    MergeTreeDataPartType type;
    auto full_path = relative_data_path + relative_path + "/";
    auto mrk_ext = MergeTreeIndexGranularityInfo::getMarksExtensionFromFilesystem(volume->getDisk(), full_path);

    if (mrk_ext)
        type = getPartTypeFromMarkExtension(*mrk_ext);
    else
    {
        /// Didn't find any mark file, suppose that part is empty.
        type = choosePartType(0, 0);
    }

    return createPart(name, type, part_info, volume, relative_path);
}





    auto reader_stream = MergeTreeReaderStream(
            disk,
            old_part_path_str + "t",
            ".bin",
            data_part->getMarksCount(),
            all_mark_ranges,
            settings,
            mark_cache,
            uncompressed_cache,
            data_part->getFileSizeOrZero(stream_name + DATA_FILE_EXTENSION),
            &data_part->index_granularity_info,
            profile_callback,
            clock_type);


    IDataType::DeserializeBinaryBulkStatePtr state;
    type.deserializeBinaryBulkStatePrefix(settings, state);
    size_t block_size = 3;
    size_t overall_size = 0;
    Field min_value;
    Field max_value;

    while (!column_read_buffer.eof())
    {
        type.deserializeBinaryBulkWithMultipleStreams(*new_column, block_size, settings, state);
        overall_size = overall_size + new_column->size();

        Field block_min_value;
        Field block_max_value;
        new_column->getExtremes(block_min_value,block_max_value);
        new_column->cloneEmpty();

        if (min_value.isNull() || block_min_value < min_value)
        {
            min_value = block_min_value;
        }

        if (max_value.isNull() || block_max_value > max_value)
        {
            max_value = block_max_value;
        }

    std::cout
        << new_column->dumpStructure()  << std::endl
        << block_min_value.get<UInt32>() << std::endl
        << block_max_value.get<UInt32>() << std::endl;
        // if (!min_value.tryGet)
        // if min_value >
    }


    std::cout
        << overall_size << std::endl
        << new_column->dumpStructure()  << std::endl
        << min_value.get<UInt32>() << std::endl
        << max_value.get<UInt32>() << std::endl;
//  1590962400

    // auto part_info = MergeTreePartInfo::fromPartName(old_part_name, MergeTreeDataFormatVersion(1));
    // std::cout << date_column << std::endl
    //           << dest_path << std::endl
    //           << part_info.getDataVersion() << std::endl
    //           << part_info.getPartName() << std::endl
    //           << part_info.getBlocksCount()<< std::endl ;



// partition.dat
    // ReadBufferFromFile checksums_in(old_part_path_str + "checksums.txt", 4096);
    // MergeTreeDataPartChecksums checksums;
    // checksums.read(checksums_in);

/ *

        streams.emplace(stream_name, std::make_unique<MergeTreeReaderStream>(
            data_part->volume->getDisk(), data_part->getFullRelativePath() + stream_name, DATA_FILE_EXTENSION,
            data_part->getMarksCount(), all_mark_ranges, settings, mark_cache,
            uncompressed_cache, data_part->getFileSizeOrZero(stream_name + DATA_FILE_EXTENSION),
            &data_part->index_granularity_info,
            profile_callback, clock_type));




void MergeTreeIndexGranuleSet::deserializeBinary(ReadBuffer & istr)
{
    block.clear();

    Field field_rows;
    const auto & size_type = DataTypePtr(std::make_shared<DataTypeUInt64>());
    size_type->deserializeBinary(field_rows, istr);
    size_t rows_to_read = field_rows.get<size_t>();

    if (rows_to_read == 0)
        return;

    for (size_t i = 0; i < index_sample_block.columns(); ++i)
    {
        const auto & column = index_sample_block.getByPosition(i);
        const auto & type = column.type;
        auto new_column = type->createColumn();

        IDataType::DeserializeBinaryBulkSettings settings;
        settings.getter = [&](IDataType::SubstreamPath) -> ReadBuffer * { return &istr; };
        settings.position_independent_encoding = false;

        IDataType::DeserializeBinaryBulkStatePtr state;
        type->deserializeBinaryBulkStatePrefix(settings, state);
        type->deserializeBinaryBulkWithMultipleStreams(*new_column, rows_to_read, settings, state);

        block.insert(ColumnWithTypeAndName(new_column->getPtr(), type, column.name));
    }
}



    std::map<String, size_t> stream_counts;
    for (const NameAndTypePair & column : source_part->getColumns())
    {
        column.type->enumerateStreams(
            [&](const IDataType::SubstreamPath & substream_path)
            {
                ++stream_counts[IDataType::getFileNameForStream(column.name, substream_path)];
            },
            {});
    }



    /// Read no more than limit values and append them into column.
    virtual void deserializeBinaryBulkWithMultipleStreams(
        IColumn & column,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & / *state* /) const
    {
        if (ReadBuffer * stream = settings.getter(settings.path))
            deserializeBinaryBulk(column, *stream, limit, settings.avg_value_size_hint);
    }



            {
                IDataType::SubstreamPath stream_path;
                name_type.type->enumerateStreams([&](const IDataType::SubstreamPath & substream_path)
                {
                    String file_name = IDataType::getFileNameForStream(name_type.name, substream_path);
                    String mrk_file_name = file_name + index_granularity_info.marks_file_extension;
                    String bin_file_name = file_name + ".bin";
                    if (!checksums.files.count(mrk_file_name))




void IMergeTreeDataPart::loadColumnsChecksumsIndexes(bool require_columns_checksums, bool check_consistency)
{
    assertOnDisk();

    /// Memory should not be limited during ATTACH TABLE query.
    /// This is already true at the server startup but must be also ensured for manual table ATTACH.
    /// Motivation: memory for index is shared between queries - not belong to the query itself.
    auto temporarily_disable_memory_tracker = getCurrentMemoryTrackerActionLock();

    loadColumns(require_columns_checksums);
    loadChecksums(require_columns_checksums);
    loadIndexGranularity();
    calculateColumnsSizesOnDisk();
    loadIndex();     /// Must be called after loadIndexGranularity as it uses the value of `index_granularity`
    loadRowsCount(); /// Must be called after loadIndexGranularity() as it uses the value of `index_granularity`.
    loadPartitionAndMinMaxIndex();
    loadTTLInfos();

    if (check_consistency)
        checkConsistency(require_columns_checksums);
}



    String new_part_name = part_info.getPartName();

    auto new_part_path = Poco::Path::forDirectory(dest_path);
    new_part_path.pushDirectory(new_part_name);
    if (Poco::File(new_part_path).exists())
        throw Exception("Destination part directory `" + new_part_path.toString() + "` already exists",
            ErrorCodes::DIRECTORY_ALREADY_EXISTS);

    DayNum min_date;
    DayNum max_date;
    MergeTreePartInfo::parseMinMaxDatesFromPartName(old_part_name, min_date, max_date);

//     /// Checksums from the rest files listed in checksums.txt. May be absent. If present, they are subsequently compared with the actual data checksums.
//     IMergeTreeDataPart::Checksums checksums_txt;

//     if (disk->exists(path + "checksums.txt"))
//     {
//         auto buf = disk->readFile(path + "checksums.txt");
//         checksums_txt.read(*buf);
//         assertEOF(*buf);
//     }


// void IMergeTreeDataPart::MinMaxIndex::load(const MergeTreeData & data, const DiskPtr & disk_, const String & part_path)

// void IMergeTreeDataPart::MinMaxIndex::store(
//     const MergeTreeData & data, const DiskPtr & disk_, const String & part_path, Checksums & out_checksums) const
// {




    UInt32 yyyymm = DateLUT::instance().toNumYYYYMM(min_date);
    if (yyyymm != DateLUT::instance().toNumYYYYMM(max_date))
        throw Exception("Part " + old_part_name + " spans different months",
            ErrorCodes::BAD_DATA_PART_NAME);

    ReadBufferFromFile checksums_in(old_part_path_str + "checksums.txt", 4096);
    MergeTreeDataPartChecksums checksums;
    checksums.read(checksums_in);

    auto date_col_checksum_it = checksums.files.find(date_column + ".bin");
    if (date_col_checksum_it == checksums.files.end())
        throw Exception("Couldn't find checksum for the date column .bin file `" + date_column + ".bin`",
            ErrorCodes::NO_FILE_IN_DATA_PART);

    UInt64 rows = date_col_checksum_it->second.uncompressed_size / DataTypeDate().getSizeOfValueInMemory();

    auto new_tmp_part_path = Poco::Path::forDirectory(dest_path);
    new_tmp_part_path.pushDirectory("tmp_convert_" + new_part_name);
    String new_tmp_part_path_str = new_tmp_part_path.toString();
    try
    {
        Poco::File(new_tmp_part_path).remove(/ * recursive = * / true);
    }
    catch (const Poco::FileNotFoundException &)
    {
        /// If the file is already deleted, do nothing.
    }
    localBackup(disk, old_part_path.toString(), new_tmp_part_path.toString(), {});

    WriteBufferFromFile count_out(new_tmp_part_path_str + "count.txt", 4096);
    HashingWriteBuffer count_out_hashing(count_out);
    writeIntText(rows, count_out_hashing);
    count_out_hashing.next();
    checksums.files["count.txt"].file_size = count_out_hashing.count();
    checksums.files["count.txt"].file_hash = count_out_hashing.getHash();

    IMergeTreeDataPart::MinMaxIndex minmax_idx(min_date, max_date);
    Names minmax_idx_columns = {date_column};
    DataTypes minmax_idx_column_types = {std::make_shared<DataTypeDate>()};
    minmax_idx.store(minmax_idx_columns, minmax_idx_column_types, disk, new_tmp_part_path_str, checksums);

    Block partition_key_sample{{nullptr, std::make_shared<DataTypeUInt32>(), makeASTFunction("toYYYYMM", std::make_shared<ASTIdentifier>(date_column))->getColumnName()}};

    MergeTreePartition partition(yyyymm);
    partition.store(partition_key_sample, disk, new_tmp_part_path_str, checksums);
    String partition_id = partition.getID(partition_key_sample);

    Poco::File(new_tmp_part_path_str + "checksums.txt").setWriteable();
    WriteBufferFromFile checksums_out(new_tmp_part_path_str + "checksums.txt", 4096);
    checksums.write(checksums_out);

    Poco::File(new_tmp_part_path).renameTo(new_part_path.toString()); */
}

}

int main(int /* argc */, char ** /* argv */)
try
{
    /*
    boost::program_options::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "produce help message")
        ("part", boost::program_options::value<std::string>()->required(),
            "part directory to convert")
        ("date-column", boost::program_options::value<std::string>()->required(),
            "name of the date column")
        ("to", boost::program_options::value<std::string>()->required(),
            "destination directory")
    ;

    boost::program_options::variables_map options;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), options);

    if (options.count("help") || options.size() < 3)
    {
        std::cout
            << "Convert a MergeTree part from the old-style month-partitioned table "
            << "(e.g. 20140317_20140323_2_2_0) to the format suitable for ATTACH'ing to a custom-partitioned "
            << "table (201403_2_2_0)." << std::endl << std::endl;
        std::cout << desc << std::endl;
        return 1;
    }
    auto part_path = options.at("part").as<DB::String>();
     ;

    auto date_column = options.at("date-column").as<DB::String>();
    auto dest_path = options.at("to").as<DB::String>();
    */


    DB::run("/home/mfilimonov/workspace/altinity/bugs/issue1692/a/20200601_1_1_0", "t", "/tmp/20200601_1_1_0");

    return 0;
}
catch (...)
{
    std::cerr << DB::getCurrentExceptionMessage(true) << '\n';
    throw;
}
