from model.PackageInfo import PackageInfo
from model.TaskType import TaskType

if __name__ == '__main__':
    package_info = PackageInfo()
    package_id_list = package_info.get_package()[TaskType.Hotel]
    for each_obj in package_id_list:
        package_info.alter_slice_num(each_obj)