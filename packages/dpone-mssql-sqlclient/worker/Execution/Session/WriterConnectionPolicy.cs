using Dpone.SqlClient.Job;
using Microsoft.Data.SqlClient;

namespace Dpone.SqlClient.Execution.Session;

internal static class WriterConnectionPolicy
{
    internal static void Admit(SqlClientCredentials credentials, bool allowDisposableTest)
    {
        if (credentials is null || (credentials.TlsProfile == "disposable_test" && !allowDisposableTest) ||
            credentials.TlsProfile is not ("verified" or "disposable_test")) throw SessionFailure.Invalid();
    }
    // This builder contains secrets. It stays within private composition/tests,
    // is never a result or diagnostic, and never supplies arbitrary SDK options.
    internal static SqlConnectionStringBuilder Build(SqlClientCredentials credentials, bool allowDisposableTest, int timeoutSeconds)
    {
        Admit(credentials, allowDisposableTest);
        if (timeoutSeconds <= 0) throw SessionFailure.Invalid();
        try
        {
            return new SqlConnectionStringBuilder
            {
                DataSource = $"tcp:{credentials.Host},{credentials.Port}", InitialCatalog = credentials.Database,
                UserID = credentials.Username, Password = credentials.Password, IntegratedSecurity = false,
                Authentication = SqlAuthenticationMethod.SqlPassword, Pooling = false, MultipleActiveResultSets = false,
                Enlist = false, ConnectRetryCount = 0, PersistSecurityInfo = false, ConnectTimeout = timeoutSeconds,
                Encrypt = SqlConnectionEncryptOption.Mandatory,
                TrustServerCertificate = credentials.TlsProfile == "disposable_test" && allowDisposableTest,
                ApplicationName = "dpone-sqlclient"
            };
        }
        catch (Exception) { throw SessionFailure.Invalid(); }
    }
}
