// Pure protocol vectors: no SQL, credentials or route certification.
SessionControlChecks.Run(args[0], args.Length > 1 ? args[1] : null);
SessionResultChecks.Run(args[0]);
