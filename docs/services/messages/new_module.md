
The micro service [ms-new-modules](https://github.com/ccdexplorer/ms-new-modules) has a subscription to the MQTT channel `heartbeat/module/new` and starts processing when a new message arrives. The message contains the `module_ref`.


## Steps

### Metadata
In this step, the service will ask the node (`GetModuleSource`) for the WebAssembly source bytes and parse this with a WebAssembly Decoder. If this succeeds, it will proceed to extract the method names from the source bytes. 

### Verification
The final step is veryfying a module. To capture this, we’ve added a verification object:
::: ccdexplorer.domain.mongo.ModuleVerification
    handler: python
    options:
        show_source: true
        show_bases: false
        show_signature: true


!!! Dockerfile
    To facilitate the verification process, both Concordium-client and cargo Concordium (and Rust) are included in the Dockerfile. 

#### Verification Steps
1. Save the module to disk. Command: `module show module_ref —-out tmp/filename.out`
2. Print build info: Command: `cargo concordium print-build-info —-module tmp/filename.out`
3. Retrieve source from url. Use `httpx` to download contents from url. 
4. Unpack downloaded tar file to disk (module_path)
5. Start verification process. Command: `cargo concordium verify-build —-module module_path`
6. Store verification results in `modules` collection

## Result
The result of the verification process is shown on the module page in the Explorer. 