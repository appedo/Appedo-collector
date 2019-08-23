package com.appedo.webcollector.webserver.manager;


import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;

import javax.mail.Authenticator;
import javax.mail.Message;
import javax.mail.Multipart;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import org.apache.log4j.Category;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.simpleemail.AWSJavaMailTransport;
import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.servlet.InitServlet;

/**
 * Class which sends email using the standard JavaMail API for Amazon Simple Email Service or normal SMTP method.
 * For Amazon Simple Email Service:
 * <p>
 * <b>Prerequisites:</b> You must have a valid Amazon Web Services developer
 * account, and be signed up to use Amazon Simple Email Service. For more information
 * on Amazon Simple Email Service, see http://aws.amazon.com/ses .
 * <p>
 * <b>Important:</b> Be sure to fill in your AWS access credentials in the
 * AwsCredentials.properties file before you try to run this sample.
 * http://aws.amazon.com/security-credentials
 */
public class AWSJavaMail {
	
	/*
	 * Important: Be sure to fill in an email address you have access to
	 *			so that you can receive the initial confirmation email
	 *			from Amazon Simple Email Service.
	 */
	private static final Category log = Category.getInstance(AWSJavaMail.class.getName());
	private static AWSJavaMail instance_ = null;
	
	//public static String SMTP_MAIL_PROPERTIES_PATH = "SmtpCredentials.properties"; 
	
	boolean mailStatus = false;
	private String strMailingOption = "";
	private String smtp_server_ip = "";
	private String smtp_port_number = "";
	private String smtp_auth_user = "";
	private String smtp_host_name = "";
	private String smtp_auth_pwd = "";
	private String smtp_bounce_forwardto = "";
	private boolean is_ssl_supported;
	public String FROM_EMAIL_ADDRESS = null;
	
	private Properties mailProps = null;
	boolean changed = false;
	
	private AWSJavaMail() {
	}
	
	/**
	 * Get the AWS object instance, which is created already.
	 * 
	 * @return
	 */
	public static AWSJavaMail getManager() {
		if( instance_ == null ) {
			instance_ = new AWSJavaMail();
		}
		
		return instance_;
	}
	/**
	 * Enum class used to identify the module/operation for which mail subject will be formatted.
	 * 
	 * @author Ramkumar R
	 *
	 */
	public static enum MODULE_ID {
		SLA_EMAIL
	}
	
	/**
	 * Loads SMTP and AWS mail configuration properties
	 * 
	 * @param strFilePath
	 * @return
	 * @throws Exception
	 */
	public boolean loadPropertyFileConstants(String strFilePath) throws Exception {
		Properties prop = new Properties();
		
		try {
			mailProps = new Properties();
			
			InputStream is = new FileInputStream(strFilePath);
			prop.load(is);
			
			this.strMailingOption = prop.getProperty("mailing_system");
			
			if( this.strMailingOption.equalsIgnoreCase("smtp") ) {
				this.smtp_server_ip = prop.getProperty("Smtpserver");
				this.smtp_port_number = prop.getProperty("portnumber");
				this.smtp_auth_user = prop.getProperty("smtpuserid");
				//this.smtp_host_name = prop.getProperty("DRIVER");
				this.smtp_auth_pwd = prop.getProperty("smtppassword");
				this.smtp_bounce_forwardto = prop.getProperty("smtp_bounce_forwardto");
				this.is_ssl_supported = Boolean.parseBoolean( prop.getProperty("is_ssl_supported") );
				this.FROM_EMAIL_ADDRESS = prop.getProperty("FROM_EMAIL_ADDRESS");
				
				mailProps.setProperty("mail.transport.protocol", "smtp");
				mailProps.setProperty("mail.smtp.port", this.smtp_port_number);
				mailProps.setProperty("mail.smtp.host", this.smtp_server_ip);
				mailProps.setProperty("mail.smtp.auth", "true");
				mailProps.setProperty("mail.smtp.from", this.smtp_bounce_forwardto);
				
				if( this.is_ssl_supported ){
					mailProps.setProperty("mail.smtp.starttls.enable", "true");
					mailProps.setProperty("mail.smtp.socketFactory.port", this.smtp_port_number);
					mailProps.setProperty("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
					mailProps.setProperty("mail.smtp.socketFactory.fallback", "false");
				}
			} else if( this.strMailingOption.equalsIgnoreCase("aws") ) {
				//this.smtp_auth_user = rst.getString("aws_mailid");
				
				/*
				 * Important: Be sure to fill in your AWS access credentials in the
				 * AwsCredentials.properties file before you try to run this sample.
				 * http://aws.amazon.com/security-credentials
				 */
				PropertiesCredentials credentials = new PropertiesCredentials(AWSJavaMail.class.getResourceAsStream("AwsCredentials.properties"));
				
				
				/*
				 * Setup JavaMail to use the Amazon Simple Email Service by
				 * specifying the "aws" protocol.
				 */
				mailProps.setProperty("mail.transport.protocol", "aws");
				
				/*
				 * Setting mail.aws.user and mail.aws.password are optional. Setting
				 * these will allow you to send mail using the static transport send()
				 * convince method.  It will also allow you to call connect() with no
				 * parameters. Otherwise, a user name and password must be specified
				 * in connect.
				 */
				mailProps.setProperty("mail.aws.user", credentials.getAWSAccessKeyId());
				mailProps.setProperty("mail.aws.password", credentials.getAWSSecretKey());
				//mailProps.put("mail.from", "qam_automail@softsmith.com");
			}
			
		} catch(Exception e) {
			LogManager.errorLog(e);
			throw e;
		}
		
		return true;
	}
	
	/**
	 * Send mail via configured either through SMTP/AWS 
	 * 
	 * @param moduleId
	 * @param map
	 * @param from
	 * @param toAddress
	 * @param subject
	 * @return
	 * @throws Exception
	 */
	public boolean sendMail(MODULE_ID moduleId, HashMap<String, Object> map, String[] toAddress, String subject) throws Exception {
		boolean bSend = false;
		StringBuilder strBody = new StringBuilder();
		BufferedReader bufferedReader = null;
		String strPath = "", strKey = null, line = null;
		Iterator<String> iter = null;
		Session session = null;
		
		try{
			if( strMailingOption.equalsIgnoreCase("smtp") ){
				Authenticator auth = new SMTPAuthenticator(smtp_auth_user,smtp_auth_pwd);
				/*
				if(changed == true){
					session = Session.getInstance(mailProps, auth);
					this.changed = false;
				}else{*/
					session = Session.getInstance(mailProps, auth);
				//}
			}else if( strMailingOption.equalsIgnoreCase("aws") ){
				session = Session.getInstance(mailProps);
				//session.setDebug(true);
			}
			
			if( moduleId == MODULE_ID.SLA_EMAIL ){
				strPath = InitServlet.realPath+"/email/APPEDO_SLA_ERROR_BREACHED_MAIL.htm";
				
				bufferedReader = new BufferedReader(new FileReader(strPath));
				
				while ((line = bufferedReader.readLine()) != null) {
					iter = map.keySet().iterator();
					
					while( iter.hasNext() ){
						strKey = iter.next();
						line = line.replaceAll("@"+strKey+"@",map.get(strKey).toString());
					}
					strBody.append(line);
				}
			}
			
			// Send's mail either through SMTP or AWS
			int nTotalRecipient = toAddress.length;
			if( nTotalRecipient>0 ){
				String [] recipients = new String[nTotalRecipient];
				
				int x = 0;
				for (int i = 0; i < toAddress.length; i++) {
					String strRecp = toAddress[i];
					if( strRecp.length() > 0 ){
						recipients[x++] = strRecp;
					}
				}
				
				// create a message
				Message msg = new MimeMessage(session);
				msg.addFrom(InternetAddress.parse(smtp_auth_user));
				
				// set the from and to address
				InternetAddress addressFrom = new InternetAddress(smtp_auth_user);
				
				msg.setFrom(addressFrom);
				//msg.setReplyTo(new InternetAddress[]{addressFrom});
				
				InternetAddress[] addressTo = new InternetAddress[nTotalRecipient];

				/* AWS Verfication for test purpose.
				AmazonSimpleEmailService email = new AmazonSimpleEmailServiceClient(credentials);		
				ListVerifiedEmailAddressesResult verifiedEmails = email.listVerifiedEmailAddresses();
				*/
				
				for (int i = 0; i < nTotalRecipient; i++) {
					/* AWS Verfication for test purpose.
					if (!verifiedEmails.getVerifiedEmailAddresses().contains(recipients[i])) {
						email.verifyEmailAddress(new VerifyEmailAddressRequest().withEmailAddress(recipients[i]));
					} */
					addressTo[i] = new InternetAddress(recipients[i]);
				}
				//msg.setRecipients(Message.RecipientType.TO, addressTo);
				
				
				Multipart multipart = new MimeMultipart();
				
				MimeBodyPart messagePart = new MimeBodyPart();
				//messagePart.setText("hi");
				messagePart.setContent(strBody.toString(), "text/html");
				
				multipart.addBodyPart(messagePart);
				
				
				msg.setSentDate(Calendar.getInstance().getTime());
				
				msg.setContent( multipart );
				//msg.setContent(strBody.toString() != null ? strBody.toString() : "", "text/html");
				
				msg.addRecipients(Message.RecipientType.TO, addressTo);
				
				msg.setSubject(subject);
				//msg.setText(strBody.toString());
				msg.saveChanges();
				
				if( this.strMailingOption.equalsIgnoreCase("smtp") ){
					Transport.send(msg);
				}else if( this.strMailingOption.equalsIgnoreCase("aws") ){
					// Reuse one Transport object for sending all your messages
					// for better performance
					Transport t = null;
					try{
						t = new AWSJavaMailTransport(session, null);
						t.connect();
						t.sendMessage(msg, null);
					}catch(Exception e){
						map.put("exception",e.getMessage());
						LogManager.errorLog(e);
						throw e;
					} finally {
						// Close your transport when you're completely done sending
						// all your messages
						try{
							t.close();
						}catch(Exception e){
							LogManager.errorLog(e);
						}
					}
				}
			}
			
		} catch (Exception e) {
			LogManager.errorLog(e);
		}finally {
			strBody = null;
			bufferedReader = null;
			strPath = "";
			strKey = null;
			line = null;
			iter = null;
			session = null;
		}
		return bSend;
	}

	/**
	 * SimpleAuthenticator is used to do simple authentication when the SMTP server requires it.
	 */
	private class SMTPAuthenticator extends Authenticator {

		private PasswordAuthentication authentication;

		/**
		 * Initialize PasswordAuthentication
		 * 
		 * @param login
		 * @param password
		 */
		public SMTPAuthenticator(String login, String password)
		{
			//System.out.println("username :"+login+"Password :"+password);
			authentication = new PasswordAuthentication(login, password);
			//System.out.println("authentication"+authentication.getPassword());
		}
		
		/**
		 * Return PasswordAuthentication
		 */
		public PasswordAuthentication getPasswordAuthentication() {
			return authentication;
		}
	}
}
