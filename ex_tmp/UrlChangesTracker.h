#pragma once
//////////////////////////////////////////////////////////////////////////

CLOUD_CTS_NAMESPACE_BEGIN

//////////////////////////////////////////////////////////////////////////

template< typename ResultCodeType >
void ThrowError( ResultCodeType result, const std::string& message, ResultCodeType success = static_cast< ResultCodeType >( 0 ) )
{
     if( CC::CallFail( result, success ) )
     {
         std::ostringstream oss;
         oss << "Error. Result code: " << result << "." << message;
         throw std::runtime_error( oss.str() );
     }
}

//////////////////////////////////////////////////////////////////////////

class UrlChangesTracker
        : public TimerEventTrackerTask
        , private CC::MIB::VariableEvents
        , private DBI::IConfig
        , private CC::CQueueThread
{
public:
    UrlChangesTracker()
        : TimerEventTrackerTask( URLS )
        , m_OutStreamBuffer( "TrackerUrlVerdictsLog_" + boost::gregorian::to_simple_string( boost::gregorian::day_clock::local_day() ) + ".txt" )
        , m_OutFileStream( &m_OutStreamBuffer )
        , m_DWSLoadStatus( DWSLoadMessage::NEED_UPDATE )
    { }

    void Start();
    void Stop();

private:
    typedef boost::shared_ptr< pqxx::connection > pqxx_connection_ptr;
    typedef std::unique_ptr< DWSI::UrlChecker > url_checker_ptr;

    virtual bool OnChangingValue( CC::MIB::Variable* pSender, CC::MIB::Value* pNewValue );
    virtual void OnChangeValue( CC::MIB::Variable* pSender, CC::MIB::Value* pOldValue, CC::MIB::Value* pNewValue );

    void Load() throw ( std::exception );

    void Init() throw ( std::exception );

    bool DwsLoaded( DWSI::UrlChecker* pUrlChecker );

    void ReloadDWSAndUpdate();

    void FindChanges( DWSI::Str2UrlCategoryPairMap_t& map, const DWSI::Str2UrlCategoryPairMap_t* pOldMap, bool is_blacklist );
    void Compare( DWSI::Str2UrlCategoryPairMap_t& new_map, const DWSI::Str2UrlCategoryPairMap_t* pOldMap, std::set< std::string >& changes );
    bool TableIsEmpty( const std::string& table_name ) throw ( std::exception );

    bool TryToReconnect();
    void PrepareQueries( pqxx_connection_ptr ptr );

    void WriteData(const DWSI::Str2UrlCategoryPairMap_t& map, const std::string& str_query );
    bool ReadData( DWSI::Str2UrlCategoryPairMap_t* pMap, const std::string& table_name );
    void Remove( const std::set< std::string >& valuesToRemove, const std::string& str_query );

    virtual std::string getHost();
    virtual std::string getPort();
    virtual std::string getDBname();
    virtual std::string getUser();
    virtual std::string getPwd();

private:
    virtual void OnInit();
    virtual void OnHandleMessage( const CC::CMessage *pMsg );
    virtual void OnDone();

private:
    CC::CSmartPtr< CC::MIB::Group > m_spDwsGroup;
    CC::CSmartPtr< CC::MIB::Variable > m_spDwsVar;

    std::set< std::string > m_chgBlack, m_chgWhite;

    DBI::CPgConnection m_pgConnection;

    boost::iostreams::stream_buffer< boost::iostreams::file_sink > m_OutStreamBuffer;
    std::ostream m_OutFileStream;

private:
    static const char* BLACK_HASH_INSERT;
    static const char* WHITE_HASH_INSERT;
    static const char* BLACK_HASH_DELETE;
    static const char* WHITE_HASH_DELETE;


#define CHNG_MSGIDS      \
    X( CHNG_MSGID_DWS_UPDATE = 2047, "CHNG_MSGID_DWS_UPDATE" ) \
    X( CHNG_MSGID_RECONNECT, "CHNG_MSGID_RECONNECT" ) \
    X( CHNG_MSGID_RESTART_COMMIT_TIMER, "CHNG_MSGID_RESTART_COMMIT_TIMER" )  \
    X( CHNG_MSGID_RESTART_DISCARD_TIMER, "CHNG_MSGID_RESTART_DISCARD_TIMER" )

#define X( a, b ) a,
    enum MsgIDs { CHNG_MSGIDS };
#undef X


private:
    class ReconnectMessage
        : public CC::CMessage
    {
    public:
        ReconnectMessage()
            : CMessage( CHNG_MSGID_RECONNECT ) {}

        ~ReconnectMessage() {}
    };

    class RestartCommitTimerMessage
        : public CC::CMessage
    {
    public:
        RestartCommitTimerMessage( CC::UINT32_t commitTime )
            : CMessage( CHNG_MSGID_RESTART_COMMIT_TIMER ), commitTime( commitTime ) {}

        ~RestartCommitTimerMessage() {}

        CC::UINT32_t commitTime;
    };

    class RestartDiscardTimerMessage
        : public CC::CMessage
    {
    public:
        RestartDiscardTimerMessage( CC::UINT32_t discardTime )
            : CMessage( CHNG_MSGID_RESTART_DISCARD_TIMER ), discardTime( discardTime ) {}

        ~RestartDiscardTimerMessage() {}

        CC::UINT32_t discardTime;
    };

    class DWSLoadMessage
        : public CC::CMessage
    {
    public:
        DWSLoadMessage()
            : CMessage( CHNG_MSGID_DWS_UPDATE ) { }

        ~DWSLoadMessage() { }

        enum LoadStatus
        {
            REPEAT_UPDATE,
            UPDATING,
            NEED_UPDATE
        };
    };

    boost::shared_mutex m_DWSLoadStatusMutex;
    DWSLoadMessage::LoadStatus m_DWSLoadStatus;
};

typedef utils::singleton< UrlChangesTracker > UrlChangeTracker;


//////////////////////////////////////////////////////////////////////////

CLOUD_CTS_NAMESPACE_END

//////////////////////////////////////////////////////////////////////