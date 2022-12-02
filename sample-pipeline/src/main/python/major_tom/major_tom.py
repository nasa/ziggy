'''
Python source code for the sample pipeline that shows off Ziggy's capabilities in
an extremely simple use-case. Each processing step in the pipeline does something simple to 
a Portable Network Graphics (PNG) formatted image file. 

This is a kind of ludicrous pipeline that does stupid things to trivial amounts of data.
Also, I'm terrible at Python programming, so this might be written in a totally awful 
Python idiom. However: the idea is that the subject matter experts would write the actual
analysis code however they feel like writing it, test it, etc., outside of the context
of the pipeline. Once that's done the process of connecting the results to Ziggy and running
them in the pipeline is straightforward. That's the theory, at least.

@author: PT
'''

from PIL import Image
import numpy as np
import os
import re

# Takes a PNG image and permutes the R, G, B components.
def permute_color(filename, throw_exception, generate_output):
    
    # If generate_output and throw_exception are both false, we can return
    # 0 here and be done with it.
    if not throw_exception and not generate_output:
        print("generate_output false, retcode zero but no output generated")
        return 
    
    # If throw_exception is true, throw an exception.
    if throw_exception:
        raise ValueError("Value error raised because throw_exception is true")
    
    # Read in the PNG file and extract the colors.
    png_file = Image.open(filename)
    
    png_array = np.array(png_file)
    red_image = np.copy(png_array[:,:,0])
    green_image = np.copy(png_array[:,:,1])
    blue_image = np.copy(png_array[:,:,2])
    
    # Permute the colors, but keep the transparency the same as in the original 
    # image.
    
    indices = np.random.permutation([0, 1, 2])
    png_array[:,:,indices[0]] = red_image
    png_array[:,:,indices[1]] = green_image
    png_array[:,:,indices[2]] = blue_image
    
    bare_filename = os.path.splitext(filename)[0];
    save_filename = bare_filename + "-perm.png"
    print("Saving color-permuted image to file {} in directory {}".format(save_filename, os.getcwd()))
    
    new_png_file = Image.fromarray(png_array, 'RGBA')
    new_png_file.save(save_filename)
            
    return

# Takes a PNG image and performs a left-right flip.
def left_right_flip(filename):
    
    # Read in the PNG file and extract the colors and the transparency.
    png_file = Image.open(filename)
    
    png_array = np.array(png_file)
    red_image = png_array[:,:,0]
    green_image = png_array[:,:,1]
    blue_image = png_array[:,:,2]
    alpha_image = png_array[:,:,3]
    
    png_array[:,:,0] = np.fliplr(red_image)
    png_array[:,:,1] = np.fliplr(green_image)
    png_array[:,:,2] = np.fliplr(blue_image)
    png_array[:,:,3] = np.fliplr(alpha_image)
    
    bare_filename = os.path.splitext(filename)[0];
    save_filename = bare_filename + "-lrflip.png"
    print("Saving LR-flipped image to file {} in directory {}".format(save_filename, os.getcwd()))
    
    new_png_file = Image.fromarray(png_array)
    new_png_file.save(save_filename)
            
    return

# Takes a PNG image and performs an up-down flip.
def up_down_flip(filename):
    
    # Read in the PNG file and extract the colors and the transparency.
    png_file = Image.open(filename)
    
    png_array = np.array(png_file)
    red_image = png_array[:,:,0]
    green_image = png_array[:,:,1]
    blue_image = png_array[:,:,2]
    alpha_image = png_array[:,:,3]
    
    png_array[:,:,0] = np.flipud(red_image)
    png_array[:,:,1] = np.flipud(green_image)
    png_array[:,:,2] = np.flipud(blue_image)
    png_array[:,:,3] = np.flipud(alpha_image)
    
    bare_filename = os.path.splitext(filename)[0];
    save_filename = bare_filename + "-udflip.png"
    print("Saving UD-flipped image to file {} in directory {}".format(save_filename, os.getcwd()))
    
    new_png_file = Image.fromarray(png_array)
    new_png_file.save(save_filename)
            
    return

# Takes the mean of a series of PNG images.
def average_images(filenames):
    
    n_images = len(filenames)
    i_image = 0
    
    # Extract the dataset string.
    pattern="(\\S+?)-(set-[0-9])-(file-[0-9])-perm-(\\S+?).png"
    match = re.search(pattern, filenames[0])
    setString = match.group(2);
    for filename in filenames:
        
        # Read in the PNG file.
        png_file = Image.open(filename)
        png_array = np.array(png_file)
        
        # Accumulate the image into sum_array.
        if i_image == 0:
            sum_array = np.copy(png_array)
        else:
            sum_array += png_array
        i_image += 1
        
    mean_array = sum_array // n_images
    save_filename = 'averaged-image-' + setString + '.png'   
       
    print("Saving averaged image to file {} in directory {}".format(save_filename, os.getcwd()))
    
    new_png_file = Image.fromarray(mean_array)
    new_png_file.save(save_filename)
            
    return
   
