<!DOCTYPE html>
<html>
<header>
    <style>
        * {
            margin: 0;
            padding: 0;
        }
        html, body {
            height: 100%;
        }
    </style>
    <script src="js/hterm_all.js"></script>
</header>
<body>
<div id="terminal"
     style="position:relative; width:100%; height:100%"></div>
</body>

<script>
    var t = null;
    function setupHterm() {
        hterm.defaultStorage = new lib.Storage.Memory();
        t = new hterm.Terminal();
        t.getPrefs().set("cursor-color", "red");
        t.getPrefs().set("cursor-blink", true);
        t.getPrefs().set("background-color", "white");
        t.getPrefs().set("font-size", 12);
        t.getPrefs().set("foreground-color", "black");

        t.onTerminalReady = function() {
            // Create a new terminal IO object and give it the foreground.
            // (The default IO object just prints warning messages about unhandled
            // things to the the JS console.)
            const io = t.io.push();

            io.onVTKeystroke = (str) => {
                // Do something useful with str here.
                // For example, Secure Shell forwards the string onto the NaCl plugin.
                app.onCommand(str);
            };

            io.sendString = io.onVTKeystroke;

            io.onTerminalResize = (columns, rows) => {
                // React to size changes here.
                // Secure Shell pokes at NaCl, which eventually results in
                // some ioctls on the host.
                app.resizeTerminal(columns, rows);
            };

            // You can call io.push() to foreground a fresh io context, which can
            // be uses to give control of the terminal to something else.  When that
            // thing is complete, should call io.pop() to restore control to the
            // previous io object.
        };

        t.decorate(document.querySelector('#terminal'));
        t.installKeyboard();
    }

    let ws = new WebSocket("ws://" + location.host + "/terminal");

    ws.onopen = () => {
        t.showOverlay("Connection established", 1000);

        ws.send(action("TERMINAL_READY"));
    }

    ws.onerror = () => {
        t.showOverlay("Connection error", 3000);
    }

    ws.onclose = () => {
        t.showOverlay("Connection closed", 3000);
    }

    ws.onmessage = (e) => {
        let data = JSON.parse(e.data);
        switch (data.type) {
            case "TERMINAL_PRINT":
                t.io.print(data.text);
        }
    }

    function action(type, data) {
        let action = Object.assign({
            type
        }, data);

        return JSON.stringify(action);
    }

    let app = {
        onTerminalInit() {
            ws.send(action("TERMINAL_INIT"));
        },
        onCommand(command) {
            ws.send(action("TERMINAL_COMMAND", {
                command
            }));
        },
        resizeTerminal(columns, rows) {
            ws.send(action("TERMINAL_RESIZE", {
                columns, rows
            }));
        },
        onTerminalReady() {
            ws.send(action("TERMINAL_READY"));
        }
    };

    // This will be whatever normal entry/initialization point your project uses.
    window.onload = function () {
        lib.init(setupHterm);
    };
</script>
</html>
