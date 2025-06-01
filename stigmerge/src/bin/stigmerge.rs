#![recursion_limit = "256"]

use anyhow::Result;
use clap::Parser;

use stigmerge::{App, Cli};

#[cfg(target_os = "android")]
use jni::{objects::JObject, InitArgsBuilder, JNIVersion, JavaVM};
use stigmerge_peer::is_cancelled;

/// Initialize native platform-specific stuff.
///
/// veilid-core has android-specific code that assumes if you're running on
/// android, you're an app which has called
/// veilid_core::veilid_core_setup_android. And if you haven't, veilid-core
/// configuration will error out.
///
/// This assumption is not valid for the case of a command-line binary like this
/// one, when running on android though. Which is kind of a hacker thing to do
/// already, but it can be done with adb shell or termux.
///
/// To work around this, we can set up android with a new JVM instance and pass
/// null for the AndroidKeyringContext. That new JVM instance requires an
/// available libjvm -- which might need to be installed separately into the
/// android execution environment. The null keyring context could result in an
/// NPE in keyring-manager, so we'll also conditionally compile android to
/// always use an insecure keyring.
///
/// These are both hacks around some assumptions in veilid-core that could be
/// improved upon by refining the android-specific codepaths there.
///
/// Another option would be, a stigmerge Android library. That library could
/// handle the initialization the same way veilid-flutter does.
#[cfg(target_os = "android")]
fn init_native() -> Result<()> {
    // Build the VM properties
    let jvm_args = InitArgsBuilder::new()
        // Pass the JNI API version (default is 8)
        .version(JNIVersion::V8)
        // You can additionally pass any JVM options (standard, like a system property,
        // or VM-specific).
        // Here we could enable some extra JNI checks useful during development
        //.option("-Xcheck:jni")
        .build()?;

    // Create a new VM.
    let jvm = JavaVM::new(jvm_args)?;

    // Attach the current thread to call into Java — see extra options in
    // "Attaching Native Threads" section.
    //
    // This method returns the guard that will detach the current thread when dropped,
    // also freeing any local references created in it
    let env = jvm.attach_current_thread()?;

    unsafe {
        veilid_core::veilid_core_setup_android(env.unsafe_clone(), JObject::null());
    }
    Ok(())
}

/// Placeholder for platform-specific native initialization.
#[cfg(not(target_os = "android"))]
fn init_native() -> Result<()> {
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    init_native()?;
    match tokio_main().await {
        Err(e) => {
            if !is_cancelled(&e) {
                eprintln!("{} error: Something went wrong", env!("CARGO_PKG_NAME"));
                Err(e)
            } else {
                Ok(())
            }
        }
        ok => ok,
    }
}

async fn tokio_main() -> Result<()> {
    let cli = Cli::parse();
    let mut app = App::new(cli)?;
    app.run().await?;
    Ok(())
}
