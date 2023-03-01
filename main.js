const { openiap } = require("@openiap/nodeapi")
var client = new openiap();
var fs = require('fs');


function getField(fields, line, name) {
    var index = fields.indexOf(name);
    if (index == -1) {
        console.log("Field finding error: " + name + " not found in " + fields)
        return "";
    }
    return line[index];
}

async function ProcessWorkitem(workitem, filename) {
    console.log(`Processing workitem id ${workitem._id} retry #${workitem.retries}`);
    if (workitem.payload == null) workitem.payload = {};
    workitem.payload.name = "Hello kitty"
    workitem.name = "Hello kitty"
    var data = fs.readFileSync(filename, 'utf8');
    var dataArray = data.split(/\r?\n/);
    var fields = dataArray[0].split(",");
    var groups = [];
    for (var i = 1; i < dataArray.length; i++) {
        var line = dataArray[i];
        var p_setSystemEntryDate = getField(fields, line, "p_setSystemEntryDate")

        var fields = line.split(",");
        if (line.startsWith("Group:")) {
            groups.push(line.substring(6).trim());
        }
    }
    fs.writeFileSync("result.csv", data)
    console.log(dataArray)

}
async function ProcessWorkitemWrapper(workitem) {
    var original = [];
    var files = fs.readdirSync(__dirname);
    files.forEach(file => {
        if (fs.lstatSync(file).isFile()) original.push(file);
    });
    try {
        var filename = "";
        for (var i = 0; i < workitem.files.length; i++) {
            const file = workitem.files[i];
            // await client.DownloadFile({id: file._id});
            fs.writeFileSync(file.filename, file.file);
            filename = file.filename;
        }
        var preserve = [];
        var files = fs.readdirSync(__dirname);
        files.forEach(file => {
            if (fs.lstatSync(file).isFile()) preserve.push(file);
        });

        await ProcessWorkitem(workitem, filename);
        workitem.state = "successful"
    } catch (error) {
        workitem.state = "retry"
        workitem.errortype = "application" // business rule will never retry / application will retry as mamy times as defined on the workitem queue"
        workitem.errormessage = error.message ? error.message : error
        workitem.errorsource = error.stack.toString()
    }
    files = fs.readdirSync(__dirname);
    files = files.filter(x => preserve.indexOf(x) == -1);
    files.forEach(file => {
        if (fs.lstatSync(file).isFile()) {
            workitem.files.push({ filename: file, file: fs.readFileSync(file) })
        }
    });
    await client.UpdateWorkitem({ workitem })
    files = fs.readdirSync(__dirname);
    files = files.filter(x => original.indexOf(x) == -1);
    files.forEach(file => {
        fs.unlinkSync(file);
    });
}
async function onConnected(client) {
    try {
        var queue = process.env.queue;
        var wiq = process.env.wiq;
        if (queue == null || queue == "") queue = wiq;
        const queuename = await client.RegisterQueue({ queuename: queue }, async (message) => {
            try {
                let workitem = null;
                let counter = 0;
                do {
                    workitem = await client.PopWorkitem({ wiq, includefiles: true, compressed: false })
                    if (workitem != null) {
                        counter++;
                        await ProcessWorkitemWrapper(workitem);
                    }
                } while (workitem != null)
                if (counter > 0) {
                    console.log(`No more workitems in ${wiq} workitem queue`)
                }
            } catch (error) {
                console.error(error)
            }
        })
        console.log("Consuming queue " + queuename);
    } catch (error) {
        console.error(error)
        // process.exit(1)
    }
}
async function main() {
    var wiq = process.env.wiq;
    var queue = process.env.queue;
    if (wiq == null || wiq == "") throw new Error("wiq environment variable is mandatory")
    if (queue == null || queue == "") queue = wiq;
    client.onConnected = onConnected;
    await client.connect();
    if (queue != null && queue != "") {
    } else {
        let counter = 1;
        do {
            let workitem = null;
            do {
                workitem = await client.PopWorkitem({ wiq })
                if (workitem != null) {
                    counter++;
                    await ProcessWorkitemWrapper(workitem);
                }
            } while (workitem != null)
            if (counter > 0) {
                counter = 0;
                console.log(`No more workitems in ${wiq} workitem queue`)
            }
            await new Promise(resolve => { setTimeout(resolve, 30000) }); // wait 30 seconds
        } while (true)
    }
}
main()