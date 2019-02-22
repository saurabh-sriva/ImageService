package com.amazonaws.lambda.imagehandler;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.imageio.ImageIO;

import org.imgscalr.Scalr;
import org.imgscalr.Scalr.Method;
import org.imgscalr.Scalr.Mode;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.util.IOUtils;

public class ImageHandler implements RequestHandler<S3Event, String> {

    private AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();
    //This is the Lambda environment parameter to retrieve the height of the thumbnail image. 
    private static final String HEIGHT = System.getenv("HEIGHT");
    //This is the Lambda environment parameter to retrieve the width of the thumbnail image. 
    private static final String WIDTH = System.getenv("WIDTH");
    //This is the Lambda environment parameter to retrieve the image type like JPG or PNG. 
    private static final String IMAGE_TYPE = System.getenv("IMAGE_TYPE");
    public ImageHandler() {}

    // Test purpose only.
    ImageHandler(AmazonS3 s3) {
        this.s3 = s3;
    }

    @Override
    public String handleRequest(S3Event event, Context context) 
    {
        context.getLogger().log("Received event: " + event);
        
        String sourceImagebucket = event.getRecords().get(0).getS3().getBucket().getName();
        String sourceImageFile = event.getRecords().get(0).getS3().getObject().getKey();
        S3ObjectInputStream s3ObjInputStream = null;
        
        try {
        	
        	AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
            S3Object response = s3.getObject(new GetObjectRequest(sourceImagebucket, sourceImageFile));
            s3ObjInputStream = response.getObjectContent();
            
            //resize the given source image to thumbnai image
            BufferedImage thumbNailImage = resizeThumbnailImage(s3ObjInputStream);
            
            //the S3 bucket directory to store the resized image
            String s3BucketDirectory = getS3BucketDirectory(sourceImageFile);
            
            //upload resized thumbnail image in S3 bucket in designated directory
            uploadThumbnailImageToS3(s3Client,thumbNailImage,s3BucketDirectory);
         
           
        } 
        catch (IOException exp) 
        {
            context.getLogger().log("Exception occured in methods getThumbnailImage or uploadThumbnailImageToS3"+exp.getMessage());
        }
        catch (Exception e) 
        {
            context.getLogger().log(String.format(
                "Error getting object %s from bucket %s. Make sure they exist and"
                + " your bucket is in the same region as this function.", sourceImageFile, sourceImagebucket)+"error ocurred"+e.getMessage());
        }
        finally
        {
        	if(s3ObjInputStream!=null)
        	{
        		try {
					s3ObjInputStream.close();
				} catch (IOException e) {
					
				}
        	}
        	
        }
        return "success";
    }

    /**
     * This method is responsible to resize the Source image to thumbnail image 
     * based on the height, width, quality parameters
     * @param s3ObjIs
     * @return
     * @throws IOException
     */
    private BufferedImage resizeThumbnailImage(S3ObjectInputStream s3ObjIs) throws IOException
    {
    	byte[] sourceImageBytes = IOUtils.toByteArray(s3ObjIs);
    	int height = Integer.parseInt(HEIGHT);
    	int width = Integer.parseInt(WIDTH);
    
        InputStream sourceImageInputStream = new ByteArrayInputStream(sourceImageBytes);
        BufferedImage sourceImage = ImageIO.read(sourceImageInputStream);
        BufferedImage thumbNailImage = Scalr.resize(sourceImage, Method.QUALITY,Mode.AUTOMATIC,height,width, Scalr.OP_ANTIALIAS);
        sourceImageInputStream.close();
       
        return thumbNailImage;
        
    }
    
    /**
     * This method will give the S3 directory name where all resized image 
     * will store
     * @param sourceFileName
     * @return
     */
	private String getS3BucketDirectory(String sourceFileName) 
	{
		//for example source file is abcdefhij.jpg
		String splitFileExtension[] = null;
		String directoryName = null;
		String fileCharacter = null;
		String dir_1 = null;
		String dir_2 = null;
		
		if(sourceFileName!=null) 
		{
		  splitFileExtension = sourceFileName.split("\\.");
		}
	
		if(splitFileExtension!=null && splitFileExtension.length>0)
		{
			fileCharacter = splitFileExtension[0];
		}
	
		if (fileCharacter != null && fileCharacter.length() >= 8) 
		{
			dir_1 = fileCharacter.substring(0, 4);
			dir_2 = fileCharacter.substring(4, 8);
			directoryName = "thumbnail/"+dir_1+"/"+dir_2+"/"+sourceFileName+"."+splitFileExtension[1];
		} 
		else 
		{
			if (fileCharacter != null && fileCharacter.length() > 4) 
			{
				dir_1 = fileCharacter.substring(0, 4);
			} 
			else 
			{
				dir_1 = fileCharacter;
			}
			
			directoryName = "thumbnail/"+dir_1+"/"+sourceFileName+"."+splitFileExtension[1];

		}
		
		return directoryName;
		
	}
	/**
	 * This method will upload the resized image in S3 directory
	 * @param s3Client
	 * @param thumbNailImage
	 * @param resizeImageFileName
	 * @throws IOException
	 */
	private void uploadThumbnailImageToS3(AmazonS3 s3Client,BufferedImage thumbNailImage,String resizeImageFileName) throws IOException 
	{
		ObjectMetadata meta = null;
		ByteArrayOutputStream thumbImageOutputStream = new ByteArrayOutputStream();
		ImageIO.write(thumbNailImage, IMAGE_TYPE, thumbImageOutputStream);
		InputStream inputStream = new ByteArrayInputStream(thumbImageOutputStream.toByteArray());
		meta = new ObjectMetadata();
		meta.setContentType("image/jpeg");
		s3Client.putObject("resize-img-bucket", resizeImageFileName, inputStream, meta);
		thumbImageOutputStream.close();
	}
}