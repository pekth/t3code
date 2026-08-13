# Loading Bitwarden sessions

T3 Code can load a Bitwarden `BW_SESSION` value into one running provider instance. The value
stays in the server process and is not written to a project file.

Run the command on the same machine as the T3 server. Replace `codex` with the configured provider
instance id when needed.

## macOS and Linux

If `t3` is installed or available through your shell:

```bash
bw unlock --raw | t3 provider-env load codex
```

If `t3` is not on `PATH`, use the bundled Node entry point from the T3 checkout or installation:

```bash
bw unlock --raw | node ./apps/server/dist/bin.mjs provider-env load codex
```

## Windows PowerShell

The current published T3 package may not include the Windows `provider-env` command yet. Do not
assume that `npx.cmd t3@latest` supports this flow until a release documents it. From the root of a
local T3 checkout that contains the Windows named-pipe implementation, run:

```powershell
bw unlock --raw | node .\apps\server\dist\bin.mjs provider-env load codex
```

PowerShell may resolve the npm command shim as `npx.cmd`. After a release includes this command,
the published-package form is:

```powershell
bw unlock --raw | npx.cmd t3@latest provider-env load codex
```

The Windows server uses a local named pipe. The macOS and Linux servers use an owner-only Unix
socket. The CLI selects the matching transport automatically.

To clear the value, run the same command with `clear` and do not pipe a secret:

```powershell
node .\apps\server\dist\bin.mjs provider-env clear codex
```

After a release includes the command, the published-package form is:

```powershell
npx.cmd t3@latest provider-env clear codex
```
