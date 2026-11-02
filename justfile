# Show available commands.
default:
    @just --list

# Build the Linux Tauri desktop and its matching gateway in release mode.
[linux]
build: check-desktop
    HUSKY=0 npm ci --no-audit --no-fund
    CARGO_TARGET_DIR="{{ justfile_directory() }}/src-tauri/target" npm run tauri -- build --config src-tauri/tauri.bundle.conf.json --no-bundle --ci -- --locked --bin conduit

# Check the tray dependency, which is loaded dynamically and absent from ldd.
[linux]
check-desktop:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! pkg-config --exists ayatana-appindicator3-0.1 && ! pkg-config --exists appindicator3-0.1; then
        printf '%s\n' \
            'Missing AppIndicator GTK3, required by the desktop tray.' \
            'Arch/CachyOS: sudo pacman -S --needed libayatana-appindicator' \
            'Debian/Ubuntu: sudo apt install libayatana-appindicator3-dev' >&2
        exit 1
    fi

# Build and install for the current user, including the application menu icon.
[linux]
install: build
    #!/usr/bin/env bash
    set -euo pipefail
    app_dir="$HOME/.local/lib/toolport"
    bin_dir="$HOME/.local/bin"
    data_dir="${XDG_DATA_HOME:-$HOME/.local/share}"
    install -d "$app_dir" "$bin_dir" "$data_dir/applications"
    for binary in conduit toolport-gateway; do
        install -m755 "src-tauri/target/release/$binary" "$app_dir/$binary.new"
        mv -f "$app_dir/$binary.new" "$app_dir/$binary"
    done
    ln -sfn "$app_dir/conduit" "$bin_dir/toolport"
    install -Dm644 src-tauri/icons/128x128.png "$data_dir/icons/hicolor/128x128/apps/toolport.png"
    printf '%s\n' \
        '[Desktop Entry]' \
        'Type=Application' \
        'Name=Toolport' \
        'Comment=One local MCP gateway for every AI client' \
        "Exec=\"$bin_dir/toolport\" %U" \
        'Icon=toolport' \
        'Terminal=false' \
        'Categories=Development;' \
        'Keywords=MCP;AI;agents;tools;' \
        'StartupNotify=true' \
        'MimeType=x-scheme-handler/toolport;x-scheme-handler/conduit;' \
        > "$data_dir/applications/com.tsout.conduit.desktop"
    if command -v update-desktop-database >/dev/null 2>&1; then
        update-desktop-database "$data_dir/applications"
    fi
    if command -v gtk-update-icon-cache >/dev/null 2>&1; then
        gtk-update-icon-cache -f -t "$data_dir/icons/hicolor"
    fi
    printf 'Installed Toolport: %s\n' "$bin_dir/toolport"
