fn main() {
    let eckit_include = std::env::var("ECKIT_INCLUDE_DIR")
        .unwrap_or_else(|_| "../../../mars-client-bundle/eckit/src".to_string());
    let eckit_build_include = std::env::var("ECKIT_BUILD_INCLUDE_DIR")
        .unwrap_or_else(|_| "../../../mars-client-bundle/build/eckit/src".to_string());
    let metkit_include = std::env::var("METKIT_INCLUDE_DIR")
        .unwrap_or_else(|_| "../../../mars-client-bundle/metkit/src".to_string());
    let metkit_build_include = std::env::var("METKIT_BUILD_INCLUDE_DIR")
        .unwrap_or_else(|_| "../../../mars-client-bundle/build/metkit/src".to_string());

    let mut build = cxx_build::bridge("src/lib.rs");
    build
        .file("src/bridge.cc")
        .include(".")
        .include("src")
        .include(&eckit_include)
        .include(&eckit_build_include)
        .include(&metkit_include)
        .include(&metkit_build_include)
        .std("c++17");

    build.compile("metkit_cxx");

    if let Ok(lib_dir) = std::env::var("METKIT_LIB_DIR") {
        println!("cargo:rustc-link-search=native={lib_dir}");
    }

    println!("cargo:rustc-link-lib=metkit");
    println!("cargo:rustc-link-lib=eckit");

    println!("cargo:rerun-if-changed=src/lib.rs");
    println!("cargo:rerun-if-changed=src/bridge.h");
    println!("cargo:rerun-if-changed=src/bridge.cc");
    println!("cargo:rerun-if-env-changed=ECKIT_INCLUDE_DIR");
    println!("cargo:rerun-if-env-changed=ECKIT_BUILD_INCLUDE_DIR");
    println!("cargo:rerun-if-env-changed=METKIT_INCLUDE_DIR");
    println!("cargo:rerun-if-env-changed=METKIT_BUILD_INCLUDE_DIR");
    println!("cargo:rerun-if-env-changed=METKIT_LIB_DIR");
}
