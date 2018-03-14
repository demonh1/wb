struct IConfig
{
    virtual std::string getHost() = 0;
    virtual std::string getPort() = 0;
    virtual std::string getDBname() = 0;
    virtual std::string getUser() = 0;
    virtual std::string getPwd() = 0;
};

//////////////////////////////////////////////////////////////////////////

typedef boost::shared_ptr< pqxx::connection > pqxx_connection_ptr;

//////////////////////////////////////////////////////////////////////////

class CPgConnection
{
public:
    CPgConnection();
    ~CPgConnection();

    bool Open( IConfig* pConfig );
    bool IsOpen() const;
    void Close();

    pqxx_connection_ptr Get() const;

private:
    mutable boost::shared_mutex m_mtxLocker;
    pqxx_connection_ptr m_spConnection;
};



/////////////////////////////////////////////////////////////
bool CPgConnection::Open( IConfig *pConfig )
{
    MIB_DUMP_FUNCTION();

    try
    {
        std::string strLogin( "host=" );
        strLogin.append( pConfig->getHost() );
        strLogin.append( " port=" );
        strLogin.append( pConfig->getPort() );
        strLogin.append( " dbname=" );
        strLogin.append( pConfig->getDBname() );
        strLogin.append( " user=" );
        strLogin.append( pConfig->getUser() );
        strLogin.append( " password=" );
        strLogin.append( pConfig->getPwd() );

        MIB_DUMP_DEBUG( "Login: " << strLogin );

        pqxx_connection_ptr spNewConnection = boost::make_shared< pqxx::connection >( strLogin );
        {
            CC::SYNC::WriteGuard lock( m_mtxLocker );
            m_spConnection.swap( spNewConnection );
        }

        return true;
    }
    catch( std::exception& err )
    {
        MIB_DUMP_ERROR( err.what() );
    }

    return false;
}

bool CPgConnection::IsOpen() const
{
    CC::SYNC::ReadGuard lock( m_mtxLocker );
    return ( 0 != m_spConnection.get() && m_spConnection->is_open() );
}

void CPgConnection::Close()
{
    CC::SYNC::WriteGuard lock( m_mtxLocker );
    m_spConnection.reset();
}

pqxx_connection_ptr CPgConnection::Get() const
{
    CC::SYNC::ReadGuard lock( m_mtxLocker );
    return m_spConnection;
}