using Microsoft.Win32.SafeHandles;

namespace Dpone.SqlClient.Startup;

/// <summary>Post-readiness descriptors; taking one transfers exclusive ownership once.</summary>
public enum StartupChannel
{
    /// <summary>Private credential input, usable only after parent durable registration.</summary>
    Credentials,
    /// <summary>Session observation output, never independent SQL verification.</summary>
    Session,
    /// <summary>One-shot grant input, distinct from launch permission.</summary>
    Grant,
    /// <summary>Raw result output; never stdout/stderr.</summary>
    Result,
    /// <summary>Already admitted readonly native input; never reopened by pathname.</summary>
    Input
}

/// <summary>Typed startup ownership transfer for the root-owned fixed worker main.</summary>
public sealed class StartupContext : IDisposable
{
    private readonly Dictionary<StartupChannel, int> descriptors;
    /// <summary>Admitted non-secret launch contract; no SQL authority.</summary>
    public StartupLaunch Launch { get; }
    internal StartupContext(StartupLaunch launch)
    {
        Launch = launch;
        descriptors = new() { [StartupChannel.Credentials] = launch.Descriptors.Credentials,
            [StartupChannel.Session] = launch.Descriptors.Session, [StartupChannel.Grant] = launch.Descriptors.Grant,
            [StartupChannel.Result] = launch.Descriptors.Result, [StartupChannel.Input] = launch.Descriptors.Input };
    }
    /// <summary>Transfer one role exactly once; the caller must close the returned handle.</summary>
    public SafeFileHandle Take(StartupChannel role)
    {
        if (!descriptors.TryGetValue(role, out int fd)) throw new InvalidOperationException("startup.role_unavailable");
        var handle = new SafeFileHandle((IntPtr)fd, ownsHandle: true);
        descriptors.Remove(role);
        return handle;
    }
    /// <summary>Close untaken descriptors once, attempting every detached close.</summary>
    public void Dispose()
    {
        int[] owned = descriptors.Values.ToArray(); descriptors.Clear(); Exception? error = null;
        foreach (int fd in owned)
            try { StartupFrame.Close(fd); } catch (Exception failure) { error ??= failure; }
        if (error is not null) throw new InvalidDataException("startup.close_failed");
    }
}

/// <summary>Actual managed startup; no SQL, credentials, grant or fake worker success.</summary>
public static class StartupBootstrap
{
    /// <summary>
    /// Validate actual deployment/guards and emit a bounded ready frame plus EOF.
    /// Inputs come only from fixed trusted composition. Failure must terminate the
    /// worker; no context is returned and no credential bytes are read. Final main,
    /// distribution producer and resource lookup remain root-owned composition.
    /// </summary>
    public static StartupContext Start(byte[] launchBody, StartupInstallation installation)
    {
        StartupLaunch launch = StartupLaunch.Parse(launchBody);
        var context = new StartupContext(launch); bool startupOwned = true;
        try
        {
            long deadline = Math.Min(launch.StartupDeadlineNs, launch.OperationDeadlineNs);
            StartupClock.RequireBefore(deadline);
            foreach (string name in Environment.GetEnvironmentVariables().Keys)
                if ((name.StartsWith("DOTNET_", StringComparison.OrdinalIgnoreCase) && name is not ("DOTNET_ROOT" or "DOTNET_ROLL_FORWARD" or "DOTNET_EnableDiagnostics")) ||
                    name.StartsWith("COMPlus_", StringComparison.OrdinalIgnoreCase) || name.StartsWith("CORECLR_", StringComparison.OrdinalIgnoreCase) || name.StartsWith("LD_", StringComparison.Ordinal) || name.StartsWith("DYLD_", StringComparison.Ordinal))
                    throw new InvalidDataException("startup.environment");
            if (Environment.GetEnvironmentVariable("DOTNET_ROOT") != installation.RuntimeRoot || Environment.GetEnvironmentVariable("DOTNET_ROLL_FORWARD") != "Disable" || Environment.GetEnvironmentVariable("DOTNET_EnableDiagnostics") != "0")
                throw new InvalidDataException("startup.environment");
            LinuxStartup.Verify(launch);
            DeploymentAdmission.Verify(installation, launch, deadline);
            byte[] ready = LinuxStartup.Verify(launch);
            StartupClock.RequireBefore(deadline);
            StartupFrame.Write(launch.Descriptors.Startup, ready, deadline);
            startupOwned = false; StartupFrame.Close(launch.Descriptors.Startup);
            StartupClock.RequireBefore(deadline);
            return context;
        }
        catch (Exception)
        {
            try { context.Dispose(); } catch (Exception) { }
            if (startupOwned) { startupOwned = false; try { StartupFrame.Close(launch.Descriptors.Startup); } catch (Exception) { } }
            throw new InvalidDataException("startup.admission_failed");
        }
    }
}
