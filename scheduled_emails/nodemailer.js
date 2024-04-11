const nodemailer = require("nodemailer");
const dotenv = require('dotenv');
dotenv.config({ path: "../.env" }); // adding the path ensures each folder will read the .env file as necessary

// First, define send settings by creating a new transporter:
const transporter = nodemailer.createTransport({
    host: "smtp.gmail.com", // SMTP server address (usually mail.your-domain.com)
    pool: true,
    port: 587, // Port for SMTP (usually 465)
    secure: false, // Usually true if connecting to port 465
    auth: {
        user: process.env.EMAIL_SENDER,
        pass: process.env.EMAIL_PASSWORD, //SET IN GMAIL UNDER 2-FACTOR AUTH
    },
});

const verifyTransporterConnection = async () => {
    // verify connection configuration
    transporter.verify(function (error, success) {
        if (error) {
            console.log(error);
        } else {
            console.log("External Email Server is ready to take our messages");
            // console.log(success);
        }
    });
};

const mailDetails = (args) => {
    //construct mail details/options object
    const mailOptions = {
        from: {
            name: "The Attendance Tracker",
            address: "callasteven@gmailc.om",
        },
        to: "callasteven@gmail.com",
        subject: "test",
        text: "test content",
        html: "test content2",
    };

    // pass mail options to sendMail
    // sendMail(args, mailOptions);
    return mailOptions;
};

const sendMail = async (mailOptions) => {
    let info;
    verifyTransporterConnection();

    try {
        info = await transporter.sendMail(mailOptions);
        console.log('Email sent: ' + info.response);
        return info;
    } catch (error) {
        console.log(error);
    } finally {
        process.exit(0); // ensure the process exits to the command line
    }
};

// sendMail(mailDetails());

module.exports = {
    mailDetails,
    sendMail,
};

// SECTION - SOURCES
// https://www.youtube.com/watch?v=QDIOBsMBEI0
// https://openjavascript.info/2023/01/10/nodemailer-tutorial-send-emails-in-node-js/#Basic%20example
//https://mailtrap.io/blog/sending-emails-with-nodemailer/
// templates https://codedmails.com/reset-emails-preview
// mock app at /Users/stevecalla/du_coding/utilities/node-mailer/index.js
