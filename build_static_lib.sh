#!/bin/zsh

# build_static_lib.sh
# Uses ONLY Apple's system clang - avoids Homebrew LLVM ABI issues
# Run from cyan-backend directory
#
# Usage: ./build_static_lib.sh [OPTIONS]
#
# Options:
#   --clean           Full cargo clean before build
#   --test            Also build test binaries (snapshot_test, network_test)
#   --unit            Run unit tests after build (no network needed)
#   --host            After build, run as snapshot host (waits for joiner)
#   --join <NODE_ID>  After build, connect to host and download snapshot
#   --skip-lib        Skip library build (only build/run tests)
#   --deploy          SCP test binary to Aria's laptop
#   --remote-join <NODE_ID>  Deploy + run join on Aria's laptop
#   --flutter         Build dylib for Flutter and copy to ~/cyan_flutter

set -e
setopt +o nomatch
export IPHONEOS_DEPLOYMENT_TARGET=18.0

# CORRECT PATH - matches what Xcode expects
XCODE_PATH="$HOME/cyan-iOS/Cyan/Libraries"

# Flutter project path
FLUTTER_PATH="$HOME/cyan_flutter"

# Remote machine (Aria's laptop)
REMOTE="mymomsaccount@Aria-Vyas-MacBook-Pro.local"
REMOTE_DIR="~/cyan-test"

# =============================================================================
# Parse arguments
# =============================================================================
DO_CLEAN=false
BUILD_TESTS=false
RUN_UNIT=false
RUN_HOST=false
RUN_JOIN=false
JOIN_NODE_ID=""
SKIP_LIB=false
DO_DEPLOY=false
REMOTE_JOIN=false
REMOTE_JOIN_NODE_ID=""
BUILD_FLUTTER=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --clean)
            DO_CLEAN=true
            shift
            ;;
        --test)
            BUILD_TESTS=true
            shift
            ;;
        --unit)
            BUILD_TESTS=true
            RUN_UNIT=true
            shift
            ;;
        --host)
            BUILD_TESTS=true
            RUN_HOST=true
            shift
            ;;
        --join)
            BUILD_TESTS=true
            RUN_JOIN=true
            JOIN_NODE_ID="$2"
            shift 2
            ;;
        --skip-lib)
            SKIP_LIB=true
            BUILD_TESTS=true
            shift
            ;;
        --deploy)
            BUILD_TESTS=true
            DO_DEPLOY=true
            shift
            ;;
        --remote-join)
            BUILD_TESTS=true
            DO_DEPLOY=true
            REMOTE_JOIN=true
            REMOTE_JOIN_NODE_ID="$2"
            shift 2
            ;;
        --flutter)
            BUILD_FLUTTER=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo ""
            echo "Usage: ./build_static_lib.sh [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --clean                  Full cargo clean before build"
            echo "  --test                   Also build test binaries"
            echo "  --unit                   Build + run unit tests (no network)"
            echo "  --host                   Build + run as snapshot host"
            echo "  --join <NODE_ID>         Build + connect to host"
            echo "  --skip-lib               Skip library, only build tests"
            echo "  --deploy                 SCP test binary to Aria's laptop"
            echo "  --remote-join <NODE_ID>  Deploy + run join on Aria's laptop"
            echo "  --flutter                Build dylib for Flutter macOS app"
            echo ""
            echo "Examples:"
            echo "  ./build_static_lib.sh                           # Just build static library"
            echo "  ./build_static_lib.sh --flutter                 # Build dylib for Flutter"
            echo "  ./build_static_lib.sh --test                    # Build library + tests"
            echo "  ./build_static_lib.sh --unit                    # Build + run unit tests"
            echo "  ./build_static_lib.sh --skip-lib --host         # Run as host"
            echo "  ./build_static_lib.sh --skip-lib --deploy       # Deploy binary to Aria"
            echo ""
            echo "Two-machine test (easiest):"
            echo "  Terminal 1: ./build_static_lib.sh --skip-lib --host"
            echo "  Terminal 2: ./build_static_lib.sh --skip-lib --remote-join <NODE_ID>"
            exit 1
            ;;
    esac
done

# =============================================================================
# Re-link LLVM on exit (success or failure)
# =============================================================================
trap 'echo "🔧 Re-linking Homebrew LLVM..."; brew link llvm 2>/dev/null || true' EXIT

echo "🦀 Building Cyan Backend (Apple Clang)..."
if [[ "$BUILD_FLUTTER" == "true" ]]; then
    echo "🦋 Flutter mode: Building dynamic library"
else
    echo "📁 Target: $XCODE_PATH (static library)"
fi

# =============================================================================
# Unlink Homebrew LLVM to avoid ABI conflicts
# =============================================================================
echo "🔧 Unlinking Homebrew LLVM..."
brew unlink llvm 2>/dev/null || true

# =============================================================================
# CRITICAL: Use Apple's system clang, NOT Homebrew LLVM
# =============================================================================

unset CC CXX AR LIBCLANG_PATH CXXFLAGS LDFLAGS CMAKE_CXX_FLAGS

export CC=/usr/bin/clang
export CXX=/usr/bin/clang++
export AR=/usr/bin/ar
export LIBCLANG_PATH="$(xcode-select -p)/Toolchains/XcodeDefault.xctoolchain/usr/lib"
export MACOSX_DEPLOYMENT_TARGET=14.0
export IPHONEOS_DEPLOYMENT_TARGET=17.0

echo "🔧 Using Apple System Clang:"
echo "   CC:  $CC"
$CC --version | head -1

# =============================================================================
# Clean if requested
# =============================================================================

if [[ "$DO_CLEAN" == "true" ]]; then
    echo "🧹 Nuclear clean..."
    cargo clean
    rm -rf ./build
    echo "✓ Clean complete"
else
    echo "⭕ Skipping clean (use --clean for full rebuild)"
fi

mkdir -p ./build

# =============================================================================
# Check Rust targets
# =============================================================================

check_target() {
    if ! rustup target list --installed | grep -q "$1"; then
        echo "Installing target: $1"
        rustup target add "$1"
    fi
}

echo "📦 Checking Rust targets..."
check_target "aarch64-apple-darwin"

# =============================================================================
# Phase-0 HEAD-fingerprint guardrail (FABLE_OVERNIGHT_PROMPT §0.2/§0.3)
#
# Two failure modes this defeats, both hit on 2026-07-07:
#   1. cargo's mtime no-op: a git checkout can leave src/*.rs OLDER than the
#      compiled .a, so `cargo build` ships stale bits in 0.6s. We fingerprint
#      HEAD + the working-tree diff; when it moves, we `touch` every crate
#      source so cargo MUST recompile.
#   2. silent stale artifacts: after the copy we ASSERT the .a actually
#      contains this build's `cyan-build-commit:` stamp, aborting loudly if not.
# =============================================================================

GIT_SHA="$(git rev-parse --short=9 HEAD 2>/dev/null || echo unknown)"
if [[ -n "$(git status --porcelain --untracked-files=no 2>/dev/null)" ]]; then
    BUILD_STAMP="${GIT_SHA}-dirty"
else
    BUILD_STAMP="${GIT_SHA}"
fi
export CYAN_BUILD_COMMIT="$BUILD_STAMP"

# Content fingerprint: HEAD + a digest of the uncommitted diff (mtime-independent).
DIFF_DIGEST="$(git diff HEAD 2>/dev/null | shasum -a 256 | cut -d' ' -f1)"
FINGERPRINT="${GIT_SHA}:${DIFF_DIGEST}"
FP_FILE="build/.build-fingerprint"
mkdir -p build
if [[ ! -f "$FP_FILE" || "$(cat "$FP_FILE" 2>/dev/null)" != "$FINGERPRINT" ]]; then
    echo "🔁 Source fingerprint moved → touching crate sources (defeats the cargo mtime no-op)"
    find src -name '*.rs' -exec touch {} + 2>/dev/null || true
    touch build.rs Cargo.toml 2>/dev/null || true
else
    echo "⭕ Source fingerprint unchanged ($FINGERPRINT)"
fi

echo "🏗  Build stamp: cyan-build-commit:${BUILD_STAMP}"

# 0.1 single source of truth: the orphan xcframework (the one the .xcodeproj does
# NOT link) must not exist — a cp to it silently tests stale bits.
ORPHAN_XCFW="$HOME/cyan-iOS/Cyan/CyanBackend.xcframework"
if [[ -e "$ORPHAN_XCFW" ]]; then
    echo "🗑  Removing ORPHAN xcframework: $ORPHAN_XCFW (only Cyan/Libraries/ is linked)"
    rm -rf "$ORPHAN_XCFW"
fi

# =============================================================================
# Build Library (unless --skip-lib)
# =============================================================================

export SDKROOT=$(xcrun --sdk macosx --show-sdk-path)
export BINDGEN_EXTRA_CLANG_ARGS="--sysroot=${SDKROOT}"

if [[ "$SKIP_LIB" == "false" ]]; then
    echo ""
    echo "🔨 Building for macOS (Apple Silicon)..."

    if [[ "$BUILD_FLUTTER" == "true" ]]; then
        # =============================================================================
        # FLUTTER BUILD: Dynamic library (dylib)
        # =============================================================================
        echo "🦋 Building DYNAMIC library for Flutter..."
        
        # Modify Cargo.toml temporarily to build cdylib
        # Or use cargo build with --lib flag
        RUSTFLAGS="-Awarnings" cargo build --release --target "aarch64-apple-darwin" 2>&1 | tee -a build.log

        if [[ ${pipestatus[1]} -ne 0 ]]; then
            echo "❌ Build FAILED - check build.log"
            exit 1
        fi

        echo "✅ macOS build succeeded"

        # =============================================================================
        # Create dylib from static lib (if dylib doesn't exist)
        # =============================================================================
        
        STATIC_LIB="target/aarch64-apple-darwin/release/libcyan_backend.a"
        DYLIB_OUTPUT="build/libcyan_core.dylib"
        
        # Check if cargo built a dylib directly
        if [[ -f "target/aarch64-apple-darwin/release/libcyan_backend.dylib" ]]; then
            echo "📦 Using cargo-built dylib..."
            cp "target/aarch64-apple-darwin/release/libcyan_backend.dylib" "$DYLIB_OUTPUT"
        else
            echo "📦 Creating dylib from static library..."
            # Create dylib from static lib
            # Note: This requires all symbols to be properly exported
            $CC -dynamiclib -all_load \
                -o "$DYLIB_OUTPUT" \
                "$STATIC_LIB" \
                -framework Security \
                -framework SystemConfiguration \
                -framework CoreFoundation \
                -lSystem \
                -lresolv \
                -arch arm64 \
                -install_name @rpath/libcyan_core.dylib \
                2>&1 || {
                    echo "⚠️ Direct dylib creation failed, trying alternative..."
                    # Alternative: just copy the .a and let Flutter handle it
                    cp "$STATIC_LIB" "build/libcyan_core.a"
                    echo "📦 Copied static library instead"
                }
        fi

        # =============================================================================
        # Copy to Flutter project
        # =============================================================================
        
        if [[ -d "$FLUTTER_PATH" ]]; then
            echo "📋 Copying to Flutter project: $FLUTTER_PATH"
            
            # Create macos/Libraries directory if needed
            mkdir -p "$FLUTTER_PATH/macos/Libraries"
            
            if [[ -f "$DYLIB_OUTPUT" ]]; then
                cp "$DYLIB_OUTPUT" "$FLUTTER_PATH/macos/Libraries/"
                echo "✅ Copied libcyan_core.dylib to Flutter"
                
                # Also copy to Runner.app if it exists (for hot reload)
                if [[ -d "$FLUTTER_PATH/build/macos/Build/Products/Debug/cyan_flutter.app" ]]; then
                    mkdir -p "$FLUTTER_PATH/build/macos/Build/Products/Debug/cyan_flutter.app/Contents/Frameworks"
                    cp "$DYLIB_OUTPUT" "$FLUTTER_PATH/build/macos/Build/Products/Debug/cyan_flutter.app/Contents/Frameworks/"
                    echo "✅ Copied to Debug build"
                fi
            fi
            
            # Create/update Podfile to link the library
            echo "📝 Updating Flutter macOS configuration..."
            
            # Check if we need to update macos/Runner/Info.plist for dylib loading
            # This is handled by Flutter's FFI, but we may need to add to RPATH
            
            echo ""
            echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            echo "🦋 FLUTTER SETUP INSTRUCTIONS"
            echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            echo ""
            echo "Add to macos/Runner.xcodeproj (Build Settings > Runpath Search Paths):"
            echo "  @executable_path/../Frameworks"
            echo "  @loader_path/../Frameworks"
            echo ""
            echo "Or add to your Podfile in macos/:"
            echo "  post_install do |installer|"
            echo "    installer.pods_project.targets.each do |target|"
            echo "      target.build_configurations.each do |config|"
            echo "        config.build_settings['LD_RUNPATH_SEARCH_PATHS'] = ["
            echo "          '\$(inherited)',"
            echo "          '@executable_path/../Frameworks',"
            echo "          '@loader_path/../Frameworks'"
            echo "        ]"
            echo "      end"
            echo "    end"
            echo "  end"
            echo ""
        else
            echo "⚠️ Flutter project not found at: $FLUTTER_PATH"
            echo "   dylib saved to: $DYLIB_OUTPUT"
        fi

    else
        # =============================================================================
        # XCODE BUILD: Static library (XCFramework) - Original behavior
        # =============================================================================
        
        RUSTFLAGS="-Awarnings" cargo build --release --target "aarch64-apple-darwin" 2>&1 | tee -a build.log

        if [[ ${pipestatus[1]} -ne 0 ]]; then
            echo "❌ Build FAILED - check build.log"
            exit 1
        fi

        echo "✅ macOS build succeeded"

        # =============================================================================
        # Create XCFramework
        # =============================================================================

        echo ""
        echo "📦 Creating XCFramework..."

        cp target/aarch64-apple-darwin/release/libcyan_backend.a build/libcyan_backend_macos.a
        rm -rf build/CyanBackend.xcframework

        xcodebuild -create-xcframework \
            -library build/libcyan_backend_macos.a \
            -output build/CyanBackend.xcframework

        echo "✅ XCFramework created"

        # =============================================================================
        # Copy to Xcode project (CORRECT LOCATION)
        # =============================================================================

        echo "📋 Copying to: $XCODE_PATH"
        mkdir -p "$XCODE_PATH"
        rm -rf "$XCODE_PATH/CyanBackend.xcframework"
        cp -R build/CyanBackend.xcframework "$XCODE_PATH/"
        echo "✅ Copied to Xcode project"

        # ── Fingerprint assertion: the COPIED library must carry THIS build's stamp ──
        COPIED_A="$(find "$XCODE_PATH/CyanBackend.xcframework" -name '*.a' | head -1)"
        if [[ -z "$COPIED_A" ]] || ! strings "$COPIED_A" | grep -q "cyan-build-commit:${BUILD_STAMP}"; then
            echo ""
            echo "❌ STALE-BUILD GUARD TRIPPED: the copied xcframework does NOT contain"
            echo "   cyan-build-commit:${BUILD_STAMP} — the bits on disk are not this HEAD."
            echo "   Re-run with --clean. Do NOT test against this artifact."
            rm -f "$FP_FILE"
            exit 1
        fi
        echo "✅ Verified: copied .a carries cyan-build-commit:${BUILD_STAMP}"
        echo "$FINGERPRINT" > "$FP_FILE"
    fi

    # =============================================================================
    # Verify symbols
    # =============================================================================

    echo ""
    echo "🔍 Verifying symbols..."
    if [[ "$BUILD_FLUTTER" == "true" ]]; then
        if [[ -f "build/libcyan_core.dylib" ]]; then
            if nm -gU build/libcyan_core.dylib | grep -q "cyan_send_command"; then
                echo "✅ dylib contains cyan_send_command symbol"
            else
                echo "⚠️ cyan_send_command not found in dylib"
            fi
        fi
    else
        if nm build/libcyan_backend_macos.a 2>/dev/null | grep -q "cyan_init"; then
            echo "✅ Build contains cyan_init symbol"
        else
            echo "❌ ERROR: cyan_init not found!"
            exit 1
        fi
    fi
else
    echo "⭕ Skipping library build (--skip-lib)"
fi

# =============================================================================
# Build Test Binaries (if --test, --unit, --host, or --join)
# =============================================================================

if [[ "$BUILD_TESTS" == "true" ]]; then
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "🧪 Building Test Binaries..."
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    echo ""
    echo "🔨 Building snapshot_test..."
    RUSTFLAGS="-Awarnings" cargo build --release --bin snapshot_test 2>&1 | tee -a build.log
    echo "✅ snapshot_test built"

    echo ""
    echo "🔨 Building network_test..."
    RUSTFLAGS="-Awarnings" cargo build --release --bin network_test 2>&1 | tee -a build.log
    echo "✅ network_test built"

    echo ""
    echo "🔨 Building delta_test..."
    RUSTFLAGS="-Awarnings" cargo build --release --bin delta_test 2>&1 | tee -a build.log
    echo "✅ delta_test built"

    # Copy to build dir for easy access
    cp target/release/snapshot_test build/ 2>/dev/null || cp target/aarch64-apple-darwin/release/snapshot_test build/
    cp target/release/network_test build/ 2>/dev/null || cp target/aarch64-apple-darwin/release/network_test build/
    cp target/release/delta_test build/ 2>/dev/null || cp target/aarch64-apple-darwin/release/delta_test build/

    echo ""
    echo "📁 Test binaries available at:"
    ls -lh build/snapshot_test build/network_test build/delta_test
fi

# =============================================================================
# Run Unit Tests (if --unit)
# =============================================================================

if [[ "$RUN_UNIT" == "true" ]]; then
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "🧪 Running Unit Tests (no network required)"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    echo ""
    echo "📋 snapshot_test unit:"
    ./build/snapshot_test unit

    echo ""
    echo "📋 network_test unit:"
    ./build/network_test unit
fi

# =============================================================================
# Run as Host (if --host)
# =============================================================================

if [[ "$RUN_HOST" == "true" ]]; then
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "📡 Starting as SNAPSHOT HOST"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
    echo "Copy the NODE ID printed below to the other machine."
    echo "On other machine run:"
    echo "  ./build_static_lib.sh --skip-lib --join <NODE_ID>"
    echo ""
    echo "Or if you SCP'd the binary:"
    echo "  ./snapshot_test join <NODE_ID>"
    echo ""

    RUST_LOG=info ./build/snapshot_test host
fi

# =============================================================================
# Run as Joiner (if --join)
# =============================================================================

if [[ "$RUN_JOIN" == "true" ]]; then
    if [[ -z "$JOIN_NODE_ID" ]]; then
        echo "❌ ERROR: --join requires a NODE_ID"
        echo "Usage: ./build_static_lib.sh --join <NODE_ID>"
        exit 1
    fi

    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "📥 Connecting to HOST as JOINER"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
    echo "Connecting to: ${JOIN_NODE_ID:0:16}..."
    echo ""

    RUST_LOG=info ./build/snapshot_test join "$JOIN_NODE_ID"
fi

# =============================================================================
# Deploy to Aria's laptop (if --deploy or --remote-join)
# =============================================================================

if [[ "$DO_DEPLOY" == "true" ]]; then
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "📤 Deploying to Aria's laptop"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
    echo "🎯 Target: $REMOTE"
    echo ""

    # Create remote directory and copy binaries
    echo "📁 Creating remote directory..."
    ssh "$REMOTE" "mkdir -p $REMOTE_DIR"

    echo "📤 Copying snapshot_test..."
    scp build/snapshot_test "$REMOTE:$REMOTE_DIR/"

    echo "📤 Copying network_test..."
    scp build/network_test "$REMOTE:$REMOTE_DIR/"

    echo "📤 Copying delta_test..."
    scp build/delta_test "$REMOTE:$REMOTE_DIR/"

    echo "✅ Deployed to $REMOTE:$REMOTE_DIR/"

    # List what's there
    ssh "$REMOTE" "ls -lh $REMOTE_DIR/"
fi

# =============================================================================
# Run join on Aria's laptop (if --remote-join)
# =============================================================================

if [[ "$REMOTE_JOIN" == "true" ]]; then
    if [[ -z "$REMOTE_JOIN_NODE_ID" ]]; then
        echo "❌ ERROR: --remote-join requires a NODE_ID"
        echo "Usage: ./build_static_lib.sh --remote-join <NODE_ID>"
        exit 1
    fi

    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "🚀 Running snapshot_test join on Aria's laptop"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
    echo "Connecting to: ${REMOTE_JOIN_NODE_ID:0:16}..."
    echo ""

    ssh -t "$REMOTE" "cd $REMOTE_DIR && RUST_LOG=info ./snapshot_test join $REMOTE_JOIN_NODE_ID"
fi

# =============================================================================
# Summary
# =============================================================================

if [[ "$RUN_HOST" == "false" && "$RUN_JOIN" == "false" && "$REMOTE_JOIN" == "false" ]]; then
    echo ""
    echo "┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓"
    echo "┃ ✅ BUILD COMPLETE                                            ┃"
    echo "┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛"

    if [[ "$SKIP_LIB" == "false" ]]; then
        echo ""
        if [[ "$BUILD_FLUTTER" == "true" ]]; then
            echo "🦋 Flutter dylib: $FLUTTER_PATH/macos/Libraries/libcyan_core.dylib"
            if [[ -f "build/libcyan_core.dylib" ]]; then
                ls -lh build/libcyan_core.dylib
            fi
        else
            echo "📁 XCFramework: $XCODE_PATH/CyanBackend.xcframework"
            find "$XCODE_PATH/CyanBackend.xcframework" -name "*.a" -exec ls -lh {} \;
        fi
    fi

    if [[ "$BUILD_TESTS" == "true" ]]; then
        echo ""
        echo "🧪 Test binaries:"
        ls -lh build/snapshot_test build/network_test build/delta_test 2>/dev/null || true
        echo ""
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo "🧪 TEST COMMANDS:"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo ""
        echo "  Unit tests (local, no network):"
        echo "    ./build_static_lib.sh --skip-lib --unit"
        echo ""
        echo "  ┌─────────────────────────────────────────────────────────┐"
        echo "  │ TWO-MACHINE TEST (easiest way):                        │"
        echo "  ├─────────────────────────────────────────────────────────┤"
        echo "  │ Terminal 1 (this machine - HOST):                      │"
        echo "  │   ./build_static_lib.sh --skip-lib --host              │"
        echo "  │                                                        │"
        echo "  │ Terminal 2 (same machine - runs on Aria's laptop):     │"
        echo "  │   ./build_static_lib.sh --skip-lib --remote-join <ID>  │"
        echo "  └─────────────────────────────────────────────────────────┘"
        echo ""
        echo "  Delta sync test:"
        echo "    Terminal 1: ./build/delta_test host"
        echo "    Terminal 2: ./build/delta_test join"
        echo ""
        echo "  Deploy only (no auto-run):"
        echo "    ./build_static_lib.sh --skip-lib --deploy"
        echo "    # Then SSH to Aria and run manually:"
        echo "    ssh $REMOTE"
        echo "    cd $REMOTE_DIR && ./snapshot_test join <NODE_ID>"
        echo ""
    fi

    if [[ "$SKIP_LIB" == "false" ]]; then
        echo ""
        if [[ "$BUILD_FLUTTER" == "true" ]]; then
            echo "📌 FLUTTER NEXT STEPS:"
            echo "   1. cd ~/cyan_flutter"
            echo "   2. flutter clean"
            echo "   3. flutter run -d macos"
        else
            echo "📌 XCODE NEXT STEPS:"
            echo "   1. In Xcode: Cmd+Shift+K (Clean Build Folder)"
            echo "   2. Cmd+B (Build) with Release scheme"
            echo "   3. ./package_cyan.sh"
        fi
    fi

    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
fi
