#include "stdafx.h"

//////////////////////////////////////////////////////////////////////////

CLOUD_CTS_NAMESPACE_BEGIN

//////////////////////////////////////////////////////////////////////////

#define DEFINE_QUERY( NAME ) const char* UrlChangesTracker::NAME = #NAME

DEFINE_QUERY( BLACK_HASH_INSERT );
DEFINE_QUERY( WHITE_HASH_INSERT );
DEFINE_QUERY( BLACK_HASH_DELETE );
DEFINE_QUERY( WHITE_HASH_DELETE );

//////////////////////////////////////////////////////////////////////////

void UrlChangesTracker::OnInit()
{
    MIB_DUMP_FUNCTION();

    try
    {
        Init();
        Load();

        ThrowError( m_spDwsVar->AddEvents( static_cast< CC::MIB::VariableEvents* >( this ) ), "Cannot add listener to 'update_trigger' variable." );
        SettingsStore::instance().listen_commit_urlmask_changes_time_sec( static_cast< CC::MIB::VariableEvents* >( this ) );
        SettingsStore::instance().listen_urlmask_discard_time_hours( static_cast< CC::MIB::VariableEvents* >( this ) );
    }
    catch( std::exception& e )
    {
        MIB_DUMP_ERROR( e.what() );
    }
}

void UrlChangesTracker::OnHandleMessage( const CC::CMessage* pMsg )
{
    switch( pMsg->GetID() )
    {
        case CHNG_MSGID_DWS_UPDATE:
        {
            {
                CC::SYNC::WriteGuard dwsStatusWriteGuard( m_DWSLoadStatusMutex );
                m_DWSLoadStatus = DWSLoadMessage::UPDATING;
            }

            ReloadDWSAndUpdate();

            CC::SYNC::UpgradeGuard dwsLoadReadGuard( m_DWSLoadStatusMutex );
            if( m_DWSLoadStatus != DWSLoadMessage::REPEAT_UPDATE )
            {
                CC::SYNC::UpgradeToWriteGuard dwsLoadWriteGuard( dwsLoadReadGuard );
                m_DWSLoadStatus = DWSLoadMessage::NEED_UPDATE;
            }
        }
        case CHNG_MSGID_RECONNECT:
        {
            m_pgConnection.Close();
            TryToReconnect();
            break;
        }
        case CHNG_MSGID_RESTART_COMMIT_TIMER:
        {
            const RestartCommitTimerMessage* ptr = static_cast< const RestartCommitTimerMessage* >( pMsg );
            RestartCommitTimer( ptr->commitTime );
            break;
        }
        case CHNG_MSGID_RESTART_DISCARD_TIMER:
        {
            const RestartDiscardTimerMessage* ptr = static_cast< const RestartDiscardTimerMessage* >( pMsg );
            RestartDiscardHashesTimer( ptr->discardTime );
            break;
        }
        default:
            MIB_DUMP_ERROR( "Not valid MSGID" );
    }
}

void UrlChangesTracker::OnDone()
{
    MIB_DUMP_FUNCTION();

    try
    {
       if( !!m_spDwsVar )
       {
         ThrowError( m_spDwsVar->RemoveEvents( static_cast< CC::MIB::VariableEvents* >( this ) ), "Cannot remove listener to 'update_trigger' variable." );
         m_spDwsVar.Release();
       }

       if( !!m_spDwsGroup )
           m_spDwsGroup.Release();

    }
    catch( std::exception& e )
    {
       MIB_DUMP_ERROR( e.what() );
    }

    Free();
}

void UrlChangesTracker::Load() throw ( std::exception )
{
    MIB_DUMP_FUNCTION();

    if( TryToReconnect() )
    {
        PrepareQueries( m_pgConnection.Get() );
    }

    url_checker_ptr spUrlChecker( new DWSI::UrlChecker() );

    if( TableIsEmpty( "black_urls_and_category" ) )
    {
        if( DwsLoaded( spUrlChecker.get() ) )
        {
            if( !spUrlChecker->GetBlackUrls().empty() )
            {
                WriteData( spUrlChecker->GetBlackUrls(), BLACK_HASH_INSERT );
            }
        }
    }

    if( TableIsEmpty( "white_urls_and_category" ) )
    {
        if( DwsLoaded( spUrlChecker.get() ) )
        {
            if( !spUrlChecker->GetWhiteUrls().empty() )
            {
                WriteData( spUrlChecker->GetWhiteUrls(), WHITE_HASH_INSERT );
            }
        }
    }
}

void UrlChangesTracker::Init() throw ( std::exception )
{
    MIB_DUMP_FUNCTION();

    TimerEventTrackerTask::Init( SettingsStore::instance().get_commit_urlmask_changes_time_sec(),
                                 SettingsStore::instance().get_urlmask_discard_time_hours().uint32() * 60 * 60,
                                 SettingsStore::instance().get_urls_changes_var_path().a_str() );

    std::string hashes_and_version = m_NMSClient.Settings()[ SettingsStore::instance().get_urls_changes_var_path().a_str() ];
    size_t pos = hashes_and_version.find( ":" );
    if( pos != std::string::npos )
    {
        hashes_and_version.erase( 0, pos + 1 );
        TimerEventTrackerTask::setHashMaskVersion( boost::lexical_cast< CC::UINT32_t >( hashes_and_version ) );
    }

    SettingsStore::instance().listen_local_db_host( static_cast< CC::MIB::VariableEvents* >( this ) );
    SettingsStore::instance().listen_local_db_port( static_cast< CC::MIB::VariableEvents* >( this ) );
    SettingsStore::instance().listen_local_db_name( static_cast< CC::MIB::VariableEvents* >( this ) );
    SettingsStore::instance().listen_local_db_user( static_cast< CC::MIB::VariableEvents* >( this ) );
    SettingsStore::instance().listen_local_db_password( static_cast< CC::MIB::VariableEvents* >( this ) );

    m_spDwsGroup = CC::MIB::Connector::GetGroup( HS_INSTANCE_NAME, SettingsStore::instance().get_hash_srv_dwsdriver_path() );

    CC::MIB::ResultCode rc = m_spDwsGroup->GetVariable( "update_trigger", &m_spDwsVar );

    if( CC::CallFail( rc ) )
    {
        MIB_THROW_EXCEPTION( "Cannot get variable 'update_trigger' from '" << m_spDwsGroup->GetPath() << "'" );
    }
}

bool UrlChangesTracker::TableIsEmpty( const std::string& table_name ) throw ( std::exception )
{
    MIB_DUMP_FUNCTION();

    if( !m_pgConnection.IsOpen() )
    {
        throw pqxx::broken_connection( "Connection has been closed." );
    }

    pqxx_connection_ptr spCon = m_pgConnection.Get();
    pqxx::transaction<> pqTransaction( *spCon );

    pqxx::result pqRes = pqTransaction.exec( "SELECT true FROM " + table_name + " LIMIT 1" );

    return pqRes.empty();
}

bool UrlChangesTracker::DwsLoaded( DWSI::UrlChecker* pUrlChecker )
{
    MIB_DUMP_FUNCTION();

    std::set< std::string > dwsFiles;

    try
    {
        CC::CSmartPtr< CC::MIB::Variable > spVar;

        ThrowError( m_spDwsGroup->GetVariable( "dws_path", &spVar ), "Cannot get variable 'dws_path' from '" + std::string( m_spDwsGroup->GetPath() ) + "'" );
        std::string strDwsPath = m_NMSClient.Settings()[ spVar->GetPath() ];

        ThrowError( m_spDwsGroup->GetVariable( "dws_extension", &spVar ), "Cannot get variable 'dws_extension' from '" + std::string( m_spDwsGroup->GetPath() ) + "'" );
        std::string strDwsFileExt =  m_NMSClient.Settings()[ spVar->GetPath() ];

        std::string strDwsFileMask = "*." + strDwsFileExt;

        if( !CC::file_help::FindFiles( strDwsPath, strDwsFileMask, dwsFiles ) )
        {
            MIB_THROW_EXCEPTION( "No ufl files." );
        }

        std::for_each( dwsFiles.begin(), dwsFiles.end(), boost::bind( &DWSI::UrlChecker::LoadFile, pUrlChecker, _1 ) );

        return true;
    }
    catch( std::exception& e )
    {
        MIB_DUMP_ERROR( e.what() );
    }

    return false;
}

void UrlChangesTracker::FindChanges( DWSI::Str2UrlCategoryPairMap_t& map, const DWSI::Str2UrlCategoryPairMap_t* pOldMap, bool is_blacklist )
{
    MIB_DUMP_FUNCTION();

    if( is_blacklist )
    {
        m_chgBlack.clear();
        Compare( map, pOldMap, m_chgBlack );
        return;
    }

    m_chgWhite.clear();
    Compare( map, pOldMap, m_chgWhite );
}

void UrlChangesTracker::Compare( DWSI::Str2UrlCategoryPairMap_t& newMap, const DWSI::Str2UrlCategoryPairMap_t* pOldMap, std::set< std::string >& changes )
{
    MIB_DUMP_FUNCTION();

    for( auto it = pOldMap->begin(), end = pOldMap->end(); it != end; ++it )
    {
        auto ret = newMap.find( it->first );
        bool newMapContainOldHash = ( ret != newMap.end() );
        bool changesSetAlreadyContainHash = changes.find( it->first ) != changes.end();

        if( newMapContainOldHash )
        {
            if( !changesSetAlreadyContainHash && ret->second.first != it->second.first )
            {
                changes.insert( it->first );
                if( SettingsStore::instance().get_allow_url_verdict_changes_log() )
                {
                    std::string hash;
                    CC::GetStringFromBinary( it->first, hash );
                    m_OutFileStream << "hash: " << hash << " change verdict from: " << it->second.first << " to: " << ret->second.first << std::endl;
                }
            }
            else
            {
                newMap.erase( it->first );
            }
        }
        else if( !changesSetAlreadyContainHash )
        {
            changes.insert( it->first );
            if( SettingsStore::instance().get_allow_url_verdict_changes_log() )
            {
                std::string hash;
                CC::GetStringFromBinary( it->first, hash );
                m_OutFileStream << "hash: " << hash << " was removed" << std::endl;
            }
        }
    }
}

void UrlChangesTracker::Start()
{
    CC::CQueueThread::Start();
}

void UrlChangesTracker::Stop()
{
    CC::CQueueThread::Stop();
}

void UrlChangesTracker::ReloadDWSAndUpdate()
{
    MIB_DUMP_FUNCTION();

    try
    {
        url_checker_ptr spUrlChecker( new DWSI::UrlChecker() );
        if( !DwsLoaded( spUrlChecker.get() ) )
        {
            MIB_THROW_EXCEPTION( "Cannot load DWS base." );
        }

        std::unique_ptr< DWSI::Str2UrlCategoryPairMap_t > spBlacklist( new DWSI::Str2UrlCategoryPairMap_t() );
        std::unique_ptr< DWSI::Str2UrlCategoryPairMap_t > spWhitelist( new DWSI::Str2UrlCategoryPairMap_t() );

        if( !ReadData( spBlacklist.get(), "black_urls_and_category" ) )
        {
            MIB_THROW_EXCEPTION( "Cannot read black_urls_and_category table." );
        }

        if( !ReadData( spWhitelist.get(), "white_urls_and_category" ) )
        {
            MIB_THROW_EXCEPTION( "Cannot read white_urls_and_category table." );
        }

        auto black_map = spUrlChecker->GetBlackUrls();
        auto white_map = spUrlChecker->GetWhiteUrls();

        size_t black_map_size = black_map.size();
        size_t white_map_size = white_map.size();

        FindChanges( black_map, spBlacklist.get(), true );
        FindChanges( white_map, spWhitelist.get(), false );

        if( SettingsStore::instance().get_allow_url_verdict_changes_log() )
        {
            m_OutFileStream << "dws update trigger" << std::endl;
        }

        bool blacklist_changed = !m_chgBlack.empty();
        bool whitelist_changed = !m_chgWhite.empty();

        if( blacklist_changed )
        {
            UpdateHashMaskAndVersion( m_chgBlack );
        }

        if( whitelist_changed )
        {
            UpdateHashMaskAndVersion( m_chgWhite );
        }

        blacklist_changed |= ( black_map_size != spBlacklist->size() );
        whitelist_changed |= ( white_map_size != spWhitelist->size() );

        if( blacklist_changed )
        {
            if( !m_chgBlack.empty() )
            {
                Remove( m_chgBlack, BLACK_HASH_DELETE );
            }

            WriteData( black_map, BLACK_HASH_INSERT );
        }

        if( whitelist_changed )
        {
            if( !m_chgWhite.empty() )
            {
                Remove( m_chgWhite, WHITE_HASH_DELETE );
            }

            WriteData( white_map, WHITE_HASH_INSERT );
        }
    }
    catch( std::exception& e )
    {
        MIB_DUMP_ERROR( e.what() );
    }
}

void UrlChangesTracker::OnChangeValue( CC::MIB::Variable* pSender, CC::MIB::Value* pOldValue, CC::MIB::Value* pNewValue )
{
    try
    {
        if( std::strcmp( pSender->GetName(), "update_trigger" ) == 0 )
        {
            bool triggered = false;
            ThrowError( pNewValue->GetBoolean( triggered ), "Trigger value cannot be read." );

            if( triggered )
            {
                CC::SYNC::UpgradeGuard dwsStatusReadGuard( m_DWSLoadStatusMutex );
                if( m_DWSLoadStatus == DWSLoadMessage::NEED_UPDATE || m_DWSLoadStatus == DWSLoadMessage::UPDATING )
                {
                    GetQueue().Push( static_cast< CC::CMessage* >( new DWSLoadMessage() ) );
                    if( m_DWSLoadStatus == DWSLoadMessage::UPDATING )
                    {
                        CC::SYNC::UpgradeToWriteGuard dwsStatusWriteGuard( dwsStatusReadGuard );
                        m_DWSLoadStatus = DWSLoadMessage::REPEAT_UPDATE;
                    }
                }
            }
        }
        else if( SettingsStore::instance().get_local_db_host().is_equal( pSender ) ||
                 SettingsStore::instance().get_local_db_port().is_equal( pSender ) ||
                 SettingsStore::instance().get_local_db_name().is_equal( pSender ) ||
                 SettingsStore::instance().get_local_db_user().is_equal( pSender ) ||
                 SettingsStore::instance().get_local_db_password().is_equal( pSender ) )
        {
            GetQueue().Push( static_cast< CC::CMessage* >( new ReconnectMessage() ) );
        }
        else if( SettingsStore::instance().get_commit_urlmask_changes_time_sec().is_equal( pSender ) )
        {
            CC::UINT32_t commitTime;
            if( pNewValue->GetUInt32( commitTime ) == CC::MIB::MIB_RC_OK  )
            {
                GetQueue().Push( static_cast< CC::CMessage* >( new RestartCommitTimerMessage( commitTime ) ) );
            }
            else
            {
                MIB_DUMP_ERROR( "Troubles with getting new values for commit time" );
            }
        }
        else if( SettingsStore::instance().get_urlmask_discard_time_hours().is_equal( pSender ) )
        {
            CC::UINT32_t discardTime;
            if( pNewValue->GetUInt32( discardTime ) == CC::MIB::MIB_RC_OK )
            {
                GetQueue().Push( static_cast< CC::CMessage* >( new RestartDiscardTimerMessage( discardTime ) ) );
            }
            else
            {
                MIB_DUMP_ERROR( "Troubles with getting new values for discard time" );
            }
        }
    }
    catch( std::exception& e )
    {
        MIB_DUMP_ERROR( e.what() );
    }
}

bool UrlChangesTracker::OnChangingValue( CC::MIB::Variable* pSender, CC::MIB::Value* pNewValue )
{
    return true;
}

bool UrlChangesTracker::TryToReconnect()
{
    MIB_DUMP_FUNCTION();

    try
    {
        if( m_pgConnection.IsOpen() )
            return true;

        if( m_pgConnection.Open( static_cast< IConfig* >( this ) ) )
        {
            PrepareQueries( m_pgConnection.Get() );
            return true;
        }
    }
    catch( std::exception& e )
    {
        MIB_DUMP_ERROR( e.what() );
    }
    return false;
}

void UrlChangesTracker::PrepareQueries( pqxx_connection_ptr ptr )
{
    MIB_DUMP_FUNCTION();

    ptr->prepare( BLACK_HASH_INSERT, SettingsStore::instance().get_insert_black_hash() );
    ptr->prepare( BLACK_HASH_DELETE, SettingsStore::instance().get_delete_black_hash() );

    ptr->prepare( WHITE_HASH_INSERT, SettingsStore::instance().get_insert_white_hash() );
    ptr->prepare( WHITE_HASH_DELETE, SettingsStore::instance().get_delete_white_hash() );
}

void UrlChangesTracker::WriteData( const DWSI::Str2UrlCategoryPairMap_t& map, const std::string& str_query )
{
    MIB_DUMP_FUNCTION();

    try
    {
        if( !m_pgConnection.IsOpen() )
        {
            throw pqxx::broken_connection( "Connection has been closed." );
        }

        pqxx_connection_ptr spCon = m_pgConnection.Get();
        pqxx::transaction<> pqTransaction( *spCon );

        for( auto it = map.begin(), end = map.end(); it != end; ++it )
        {
            std::string out;
            CC::GetStringFromBinary( it->first, out );
            CC::INT32_t category  = it->second.first;

            pqTransaction.prepared( str_query )( out )( category ).exec();
        }

        pqTransaction.commit();
    }
    catch( pqxx::broken_connection& e )
    {
        MIB_DUMP_ERROR( e.what() );
    }
    catch( std::exception& e )
    {
        MIB_DUMP_ERROR( e.what() );
    }
}

bool UrlChangesTracker::ReadData( DWSI::Str2UrlCategoryPairMap_t* pMap, const std::string& table_name )
{
    MIB_DUMP_FUNCTION();

    try
    {
        if( !m_pgConnection.IsOpen() )
        {
            throw pqxx::broken_connection( "Connection has been closed." );
        }

        pqxx_connection_ptr spCon = m_pgConnection.Get();
        pqxx::transaction<> pqTransaction( *spCon );

        pqxx::result pqRes = pqTransaction.exec( "SELECT * FROM " + table_name );

        if( !pqRes.empty() )
        {
            for( auto row = pqRes.begin(); row != pqRes.end(); ++row )
            {
                const std::string& in = row[ "hash" ].as< std::string >();
                std::string out;
                pMap->insert( std::make_pair( CC::GetBinaryFromString( in, out ), std::make_pair( row[ "category" ].as< int >(), 1 ) ) );
            }
            return true;
        }
    }
    catch( pqxx::broken_connection& e )
    {
        MIB_DUMP_ERROR( e.what() );
    }
    catch( std::exception& e )
    {
        MIB_DUMP_ERROR( e.what() );
    }

    return false;
}

void UrlChangesTracker::Remove( const std::set<std::string>& valuesToRemove, const std::string& str_query )
{
    MIB_DUMP_FUNCTION();

    try
    {
        if( !m_pgConnection.IsOpen() )
        {
            throw pqxx::broken_connection( "Connection has been closed." );
        }

        pqxx_connection_ptr spCon = m_pgConnection.Get();
        pqxx::transaction<> pqTransaction( *spCon );

        for( auto it = valuesToRemove.begin(), end = valuesToRemove.end(); it != end; ++it )
        {
            std::string out;
            CC::GetStringFromBinary( *it, out );
            pqTransaction.prepared( str_query )( out ).exec();
        }

        pqTransaction.commit();
    }
    catch( pqxx::broken_connection& e )
    {
        MIB_DUMP_ERROR( e.what() );
    }
    catch( std::exception& e )
    {
        MIB_DUMP_ERROR( e.what() );
    }
}

std::string UrlChangesTracker::getHost()
{
    return SettingsStore::instance().get_local_db_host().a_str();
}

std::string UrlChangesTracker::getPort()
{
    return SettingsStore::instance().get_local_db_port().a_str();
}

std::string UrlChangesTracker::getDBname()
{
    return SettingsStore::instance().get_local_db_name().a_str();
}

std::string UrlChangesTracker::getUser()
{
    return SettingsStore::instance().get_local_db_user().a_str();
}

std::string UrlChangesTracker::getPwd()
{
    return SettingsStore::instance().get_local_db_password().a_pwd();
}

//////////////////////////////////////////////////////////////////////////

CLOUD_CTS_NAMESPACE_END
