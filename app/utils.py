from configs import Config

def return_cropped_face(img, box):
    # Getting the size of head rectangle
    print('bbox:', box)
    height_y = box[3] - box[1]
    width_x = box[2] - box[0]
    # Calculating cropping area
    if height_y > Config.min_head_size:
        center_y = box[1] + ((box[3] - box[1])/2)
        center_x = box[0] + ((box[2] - box[0])/2)
        rect_y = int(center_y - height_y/2)
        rect_x = int(center_x - width_x/2)

        # height side
        y_start = 0
        im_height = img.shape[0]
        y_end = rect_y + height_y

        if max(0, rect_y-Config.crop_extender) > 0:
            y_start = rect_y - Config.crop_extender
        if rect_y+height_y+Config.crop_extender > im_height:
            while y_end <= im_height:
                y_end = y_end + 1
        else:
            y_end = rect_y+height_y+Config.crop_extender
        # width side
        x_start = 0
        im_width = img.shape[1]
        x_end = rect_x+width_x
        if max(0, rect_x-Config.crop_extender):
            x_start = rect_x - Config.crop_extender
        if rect_x+width_x+Config.crop_extender > im_width:
            while x_end <= im_width:
                x_end = x_end + 1
        else:
            x_end = rect_x+width_x+Config.crop_extender

        cropped_img = img[y_start:y_end, x_start:x_end]
        # print('cropped_img:', cropped_img.shape[0])
        # save crop and aligned image
        # cv2.imwrite(new_img_folder+'/crop_'+str(i)+'.jpg', cropped_img)
        # face_count += 1
    else:
        return None
            # print('Face is too small or modified or in sharp angle')
    # save original image
    # if face_count > 0:
    #     cv2.imwrite(new_img_folder+'/original.jpg', img)
    return cropped_img