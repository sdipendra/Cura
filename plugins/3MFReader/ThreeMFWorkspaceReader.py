# Copyright (c) 2018 Ultimaker B.V.
# Cura is released under the terms of the LGPLv3 or higher.

from collections import defaultdict
from configparser import ConfigParser
import zipfile
import os
import threading
import urllib.parse

import xml.etree.ElementTree as ET

from UM.Workspace.WorkspaceReader import WorkspaceReader
from UM.Application import Application

from UM.Logger import Logger
from UM.i18n import i18nCatalog
from UM.Settings.ContainerStack import ContainerStack
from UM.Settings.DefinitionContainer import DefinitionContainer
from UM.Settings.InstanceContainer import InstanceContainer
from UM.Settings.ContainerRegistry import ContainerRegistry
from UM.MimeTypeDatabase import MimeTypeDatabase
from UM.Preferences import Preferences

from .WorkspaceDialog import WorkspaceDialog

from cura.Settings.ExtruderStack import ExtruderStack
from cura.Settings.GlobalStack import GlobalStack
from cura.Settings.CuraContainerStack import _ContainerIndexes
from cura.CuraApplication import CuraApplication

i18n_catalog = i18nCatalog("cura")


#
# HACK:
#
# In project loading, when override the existing machine is selected, the stacks and containers that are correctly
# active in the system will be overridden at runtime. Because the project loading is done in a different thread than
# the Qt thread, something else can kick in the middle of the process. One of them is the rendering. It will access
# the current stacks and container, which have not completely been updated yet, so Cura will crash in this case.
#
# This "@call_on_qt_thread" decorator makes sure that a function will always be called on the Qt thread (blocking).
# It is applied to the read() function of project loading so it can be guaranteed that only after the project loading
# process is completely done, everything else that needs to occupy the QT thread will be executed.
#
class InterCallObject:
    def __init__(self):
        self.finish_event = threading.Event()
        self.result = None


def call_on_qt_thread(func):
    def _call_on_qt_thread_wrapper(*args, **kwargs):
        def _handle_call(ico, *args, **kwargs):
            ico.result = func(*args, **kwargs)
            ico.finish_event.set()
        inter_call_object = InterCallObject()
        new_args = tuple([inter_call_object] + list(args)[:])
        CuraApplication.getInstance().callLater(_handle_call, *new_args, **kwargs)
        inter_call_object.finish_event.wait()
        return inter_call_object.result

    # If we are already on the main thread, return the original function
    if threading.current_thread().__class__.__name__ == "_MainThread":
        return func
    return _call_on_qt_thread_wrapper


##    Base implementation for reading 3MF workspace files.
class ThreeMFWorkspaceReader(WorkspaceReader):
    def __init__(self):
        super().__init__()
        self._supported_extensions = [".3mf"]
        self._dialog = WorkspaceDialog()
        self._3mf_mesh_reader = None
        self._container_registry = ContainerRegistry.getInstance()

        # suffixes registered with the MineTypes don't start with a dot '.'
        self._definition_container_suffix = "." + ContainerRegistry.getMimeTypeForContainer(DefinitionContainer).preferredSuffix
        self._material_container_suffix = None  # We have to wait until all other plugins are loaded before we can set it
        self._instance_container_suffix = "." + ContainerRegistry.getMimeTypeForContainer(InstanceContainer).preferredSuffix
        self._container_stack_suffix = "." + ContainerRegistry.getMimeTypeForContainer(ContainerStack).preferredSuffix
        self._extruder_stack_suffix = "." + ContainerRegistry.getMimeTypeForContainer(ExtruderStack).preferredSuffix
        self._global_stack_suffix = "." + ContainerRegistry.getMimeTypeForContainer(GlobalStack).preferredSuffix

        self._resolve_strategies = dict()

        self._resetState()

    def _resetState(self):
        self._info_dict_by_type = None
        self._all_info_dict = None
        self._validation_result_dict = None
        self._machine_info_dict = None
        self._summary_dict = None

    #
    # Separates the given file list into a list of GlobalStack files and a list of ExtruderStack files.
    # In old versions, extruder stack files have the same suffix as container stack files ".stack.cfg".
    #
    def _determineGlobalAndExtruderStackFiles(self, project_file_name, file_list):
        archive = zipfile.ZipFile(project_file_name, "r")

        global_stack_file_list = [name for name in file_list if name.endswith(self._global_stack_suffix)]
        extruder_stack_file_list = [name for name in file_list if name.endswith(self._extruder_stack_suffix)]

        # separate container stack files and extruder stack files
        files_to_determine = [name for name in file_list if name.endswith(self._container_stack_suffix)]
        for file_name in files_to_determine:
            # FIXME: HACK!
            # We need to know the type of the stack file, but we can only know it if we deserialize it.
            # The default ContainerStack.deserialize() will connect signals, which is not desired in this case.
            # Since we know that the stack files are INI files, so we directly use the ConfigParser to parse them.
            serialized = archive.open(file_name).read().decode("utf-8")
            stack_config = ConfigParser(interpolation = None)
            stack_config.read_string(serialized)

            # sanity check
            if not stack_config.has_option("metadata", "type"):
                Logger.log("e", "%s in %s doesn't seem to be valid stack file", file_name, project_file_name)
                continue

            stack_type = stack_config.get("metadata", "type")
            if stack_type == "extruder_train":
                extruder_stack_file_list.append(file_name)
            elif stack_type == "machine":
                global_stack_file_list.append(file_name)
            else:
                Logger.log("w", "Unknown container stack type '%s' from %s in %s",
                           stack_type, file_name, project_file_name)

        if len(global_stack_file_list) != 1:
            raise RuntimeError("More than one global stack file found: [%s]" % str(global_stack_file_list))

        return global_stack_file_list[0], extruder_stack_file_list

    def _loadFileAndValidate(self, archive, file_name, class_type, material_id_list):
        serialized = archive.open(file_name).read().decode("utf-8")
        file_name = os.path.basename(file_name)
        mime_type = MimeTypeDatabase.getMimeTypeForFile(file_name)
        container_id = urllib.parse.unquote_plus(mime_type.stripExtension(os.path.basename(file_name)))
        # Upgrade the serialized data
        serialized = class_type._updateSerialized(serialized, file_name)

        parser = ConfigParser(interpolation = None)
        parser.read_string(serialized)

        # qualities and variants are not upgraded
        if parser["metadata"]["type"] not in ("quality", "variant"):
            # Check if upgrade was successful
            if not parser.has_option("general", "version"):
                raise RuntimeError("%s failed to be upgraded: missing 'general/version'" % file_name)
            container_version = parser.getint("general", "version")
            if container_version != class_type.Version:
                raise RuntimeError("%s failed to be upgraded: version '%s' is not the latest '%s'" %
                                   (file_name, container_version, class_type.Version))
            if not parser.has_option("metadata", "setting_version"):
                raise RuntimeError("%s failed to be upgraded: missing 'metadata/setting_version'" % file_name)
            container_setting_version = parser.getint("metadata", "setting_version")
            if container_setting_version != CuraApplication.SettingVersion:
                raise RuntimeError("%s failed to be upgraded: version '%s' is not the latest '%s'" %
                                   (file_name, container_setting_version, CuraApplication.SettingVersion))
        elif parser["metadata"]["type"] in ("machine", "extruder_train"):
            # We will not load qualities and variants from the project file, but using the built-in ones.
            # So, to validate if this is correct, we need to check if a given stack file contains quality and variant
            # containers that cannot be found in the system.
            variant_id = parser["containers"][_ContainerIndexes.Variant]
            quality_id = parser["containers"][_ContainerIndexes.Quality]
            material_id = parser["containers"][_ContainerIndexes.Material]
            variant_metadata_list = self._container_registry.findContainersMetadata(id = variant_id, type = "variant")
            if not variant_metadata_list:
                raise RuntimeError("variant ID '%s' for stack ID '%s' cannot be found in the system." %
                                   (variant_id, container_id))
            if not self._container_registry.findContainersMetadata(id = quality_id, type = "quality"):
                raise RuntimeError("quality ID '%s' for stack ID '%s' cannot be found in the system." %
                                   (quality_id, container_id))
            if material_id not in material_id_list:
                if not self._container_registry.findContainersMetadata(id = material_id, type = "material"):
                    raise RuntimeError("material ID '%s' for stack ID '%s' cannot be found." %
                                       (material_id, container_id))

            variant_metadata = variant_metadata_list[0]
            container_type = parser["metadata"]["type"]
            variant_type = variant_metadata["hardware_type"]
            if container_type == "machine" and variant_type != "buildplate":
                raise RuntimeError("Got variant '%s' type '%s' for global stack, but only 'buildplate' is allowed" %
                                   (variant_id, variant_type))
            if container_type == "extruder_train" and variant_type != "nozzle":
                raise RuntimeError("Got variant '%s' type '%s' for extruder stack, but only 'nozzle' is allowed" %
                                   (variant_id, variant_type))

        container_type = parser["metadata"]["type"]
        if container_type not in ("machine", "extruder_train", "definition_changes", "variant", "quality",
                                  "quality_changes", "user"):
            raise RuntimeError("%s has unknown type '%s'" % (file_name, container_type))

        return {"id": container_id,
                "file_name": file_name,
                "container_type": parser["metadata"]["type"],
                "parser": parser}

    def _loadFilesAndValidate(self, archive):
        file_list = [name for name in archive.namelist() if name.startswith("Cura/")]

        global_stack_file_list = [name for name in file_list if name.endswith(self._global_stack_suffix)]
        extruder_stack_file_list = [name for name in file_list if name.endswith(self._extruder_stack_suffix)]
        xml_material_profile = self._getXmlProfileClass()
        if self._material_container_suffix is None:
            self._material_container_suffix = ContainerRegistry.getMimeTypeForContainer(xml_material_profile).preferredSuffix
        material_file_list = [name for name in file_list if name.endswith(self._material_container_suffix)]
        material_id_list = []
        if material_file_list:
            mime_type = MimeTypeDatabase.getMimeTypeForFile(material_file_list[0])
            for file_name in material_file_list:
                container_id = urllib.parse.unquote_plus(mime_type.stripExtension(os.path.basename(file_name)))
                material_id_list.append(container_id)
        container_file_list = [name for name in file_list if name.endswith(self._instance_container_suffix)]

        stack_files_to_determine = [name for name in file_list if name.endswith(self._container_stack_suffix)]

        # separate container stack files and extruder stack files
        for file_name in stack_files_to_determine:
            # FIXME: HACK!
            # We need to know the type of the stack file, but we can only know it if we deserialize it.
            # The default ContainerStack.deserialize() will connect signals, which is not desired in this case.
            # Since we know that the stack files are INI files, so we directly use the ConfigParser to parse them.
            serialized = archive.open(file_name).read().decode("utf-8")
            stack_config = ConfigParser(interpolation = None)
            stack_config.read_string(serialized)

            # sanity check
            if not stack_config.has_option("metadata", "type"):
                raise RuntimeError("%s in %s doesn't seem to be a valid stack file" % file_name)

            stack_type = stack_config["metadata"]["type"]
            if stack_type == "extruder_train":
                extruder_stack_file_list.append(file_name)
            elif stack_type == "machine":
                global_stack_file_list.append(file_name)
            else:
                raise RuntimeError("Unknown container stack type '%s' from %s" % (stack_type, file_name))

        if len(global_stack_file_list) != 1:
            raise RuntimeError("Found %s global_stack files but only one is expected." % len(global_stack_file_list))

        containers_to_load_dict = {GlobalStack: global_stack_file_list,
                                   ExtruderStack: extruder_stack_file_list,
                                   InstanceContainer: container_file_list}

        all_info_dict = dict()
        info_dict_by_type = defaultdict(dict)
        info_dict_by_type["material"] = material_file_list
        for class_type, file_list in containers_to_load_dict.items():
            for file_name in file_list:
                container_info = self._loadFileAndValidate(archive, file_name, class_type, material_id_list)
                container_type = container_info["container_type"]
                container_id = container_info["id"]
                info_dict_by_type[container_type][container_id] = container_info
                all_info_dict[container_id] = container_info

        # Validate this machine
        validation_result_dict = self._validateMachineInfo(info_dict_by_type, all_info_dict)

        return info_dict_by_type, all_info_dict, validation_result_dict

    def _validateMachineInfo(self, info_dict_by_type, all_info_dict) -> dict:
        result_dict = {"need_add_single_extruder": False}

        # Validate this machine
        machine_count = len(info_dict_by_type["machine"])
        if machine_count != 1:
            raise RuntimeError("Found %s global_stack files but only one is expected." % machine_count)
        machine_info_dict = list(info_dict_by_type["machine"].values())[0]
        machine_parser = machine_info_dict["parser"]
        machine_definition_id = machine_parser["containers"][str(_ContainerIndexes.Definition)]
        machine_definition_metadata_list = self._container_registry.findDefinitionContainersMetadata(id = machine_definition_id)
        if not machine_definition_metadata_list:
            raise RuntimeError("Cannot find definition '%s' for this machine", machine_definition_id)
        machine_definition_metadata = machine_definition_metadata_list[0]
        extruders_in_definition = machine_definition_metadata.get("machine_extruder_trains")

        # Check if extruder stacks match
        actual_extruder_stack_count = len(info_dict_by_type["extruder_train"])
        expected_extruder_stack_count = len(extruders_in_definition)
        if actual_extruder_stack_count != expected_extruder_stack_count:
            # It can be the case that the machine is single-extrusion and doesn't have an extruder (from old versions).
            # In this case, we should apply a fix.
            if actual_extruder_stack_count == 0 and expected_extruder_stack_count == 1:
                # TODO: take a note that a fix is required?
                result_dict["need_add_single_extruder"] = True
            else:
                raise RuntimeError("Expected %s extruder stacks but got %s instead" %
                                   (expected_extruder_stack_count, actual_extruder_stack_count))

        # Validate extruder stacks
        positions_seen = dict()
        for extruder_info_dict in info_dict_by_type["extruder_train"].values():
            container_id = extruder_info_dict["id"]
            parser = extruder_info_dict["parser"]
            position = parser["metadata"]["position"]
            if position not in extruders_in_definition:
                raise RuntimeError("Invalid extruder stack '%s': position '%s' doesn't exist in machine definition" %
                                   (container_id, position))
            if position in positions_seen:
                container_list = [container_id, positions_seen[position]]
                raise RuntimeError("Duplicated position '%s' found: %s" % (position, ", ".join(container_list)))
            positions_seen[position] = container_id
            expected_extruder_definition_id = extruders_in_definition[position]
            actual_extruder_definition_id = parser["containers"][str(_ContainerIndexes.Definition)]
            if expected_extruder_definition_id != actual_extruder_definition_id:
                raise RuntimeError(
                    "Invalid extruder stack '%s': definition '%s' doesn't match the machine definition '%s'" %
                    (container_id, actual_extruder_definition_id, expected_extruder_definition_id))

        # Check all stacks and make sure that the user/quality_changes/definition_changes containers they depend on
        # exist.
        all_stack_dict = list(info_dict_by_type["machine"].values())
        all_stack_dict += list(info_dict_by_type["extruder_train"].values())
        allowed_empty_containers = ("empty", "empty_definition_changes", "empty_quality_changes")
        seen_container_id_dict = dict()
        indices_to_check = (_ContainerIndexes.DefinitionChanges, _ContainerIndexes.QualityChanges,
                            _ContainerIndexes.UserChanges)
        indices_to_check = (str(idx) for idx in indices_to_check)
        for stack_info in all_stack_dict:
            stack_id = stack_info["id"]
            parser = stack_info["parser"]

            for idx in indices_to_check:
                container_id = parser["containers"][idx]
                if container_id in seen_container_id_dict:
                    usage_list = [stack_id, seen_container_id_dict[container_id]]
                    raise RuntimeError("Container '%s' is used by multiple stacks: %s" %
                                       (container_id, ", ".join(usage_list)))
                seen_container_id_dict[container_id] = stack_id
                if container_id not in all_info_dict and container_id not in allowed_empty_containers:
                    raise RuntimeError("Invalid stack '%s': cannot find container '%s' it depends on" %
                                       (stack_id, container_id))
        return result_dict

    def _loadSummaryInfoFromPreferences(self, archive, summary_dict):
        preferences_file_name = "Cura/preferences.cfg"
        visible_settings_count = 0
        try:
            temp_preferences = Preferences()
            serialized = archive.open("Cura/preferences.cfg").read().decode("utf-8")
            temp_preferences.deserialize(serialized)

            visible_settings_string = temp_preferences.getValue("general/visible_settings")
            has_visible_settings = visible_settings_string is not None
            if has_visible_settings:
                visible_settings_count = len(visible_settings_string.split(";"))
            active_mode = temp_preferences.getValue("cura/active_mode")
            if not active_mode:
                active_mode = Preferences.getInstance().getValue("cura/active_mode")
        except KeyError:
            # If there is no preferences file, it's not a workspace, so notify user of failure.
            raise RuntimeError("Failed to load preferences file %s", preferences_file_name)

        summary_dict["has_visible_settings"] = has_visible_settings
        summary_dict["visible_settings_count"] = visible_settings_count
        summary_dict["active_mode"] = active_mode

    def __fetchQualityInfo(self, container_info_dict, all_info_dict, machine_definition) -> dict:
        machine_info = list(container_info_dict["machine"].values())[0]

        quality_changes_id = machine_info["parser"]["containers"][str(_ContainerIndexes.QualityChanges)]
        if quality_changes_id == "empty_quality_changes":
            # No quality_changes, just get quality
            quality_id = machine_info["parser"]["containers"][str(_ContainerIndexes.Quality)]
            result = dict()
            if quality_id != "empty_quality":
                quality_info = all_info_dict[quality_id]
                quality_type = quality_info["parser"]["metadata"]["quality_type"]
                result = {"name": quality_type,
                          "quality_type": quality_type,
                          "is_custom_quality": False}
            return result
        quality_changes_info = all_info_dict[quality_changes_id]
        quality_changes_name = quality_changes_info["parser"]["general"]["name"]
        quality_type = quality_changes_info["parser"]["metadata"]["quality_type"]

        quality_changes_result = {"name": quality_changes_name,
                                  "quality_type": quality_type,
                                  "is_custom_quality": True,
                                  "total_settings": 0,
                                  "machine": dict(),
                                  "extruders": defaultdict(dict)}
        total_settings = 0
        for key, value in quality_changes_info["parser"]["values"].items():
            if machine_definition.getProperty(key, "settable_per_extruder"):
                quality_changes_result["extruders"]["0"][key] = value
            else:
                quality_changes_result["machine"][key] = value
            total_settings += 1
        for extruder_info in container_info_dict["extruder_train"].values():
            position = extruder_info["parser"]["metadata"]["position"]
            quality_changes_id = extruder_info["parser"]["containers"][str(_ContainerIndexes.QualityChanges)]
            quality_changes_info = all_info_dict[quality_changes_id]

            for key, value in quality_changes_info["parser"]["values"].items():
                quality_changes_result["extruders"][position][key] = value
                total_settings += 1

        quality_changes_result["total_settings"] = total_settings
        return quality_changes_result

    def __fetchUserChangesInfo(self, container_info_dict, all_info_dict, machine_definition) -> dict:
        machine_info = list(container_info_dict["machine"].values())[0]

        user_changes_id = machine_info["parser"]["containers"][str(_ContainerIndexes.UserChanges)]
        user_changes_info = all_info_dict[user_changes_id]

        user_changes_result = {"total_settings": 0,
                               "machine": dict(),
                               "extruders": defaultdict(dict)}
        total_settings = 0
        for key, value in user_changes_info["parser"]["values"].items():
            if machine_definition.getProperty(key, "settable_per_extruder"):
                user_changes_result["extruders"]["0"][key] = value
            else:
                user_changes_result["machine"][key] = value
            total_settings += 1
        for extruder_info in container_info_dict["extruder_train"].values():
            position = extruder_info["parser"]["metadata"]["position"]
            quality_changes_id = extruder_info["parser"]["containers"][str(_ContainerIndexes.UserChanges)]
            quality_changes_info = all_info_dict[quality_changes_id]

            for key, value in quality_changes_info["parser"]["values"].items():
                user_changes_result["extruders"][position][key] = value
                total_settings += 1

        user_changes_result["total_settings"] = total_settings
        return user_changes_result

    def __fetchMaterialsInfo(self, container_info_dict, all_info_dict, machine_definition) -> dict:
        machine_info = list(container_info_dict["machine"].values())[0]
        stack_info_list = [machine_info] + list(container_info_dict["extruder_train"].values())

        material_result = {"machine": dict(),
                           "extruders": defaultdict(dict)}
        for idx, stack_info in enumerate(stack_info_list):
            is_machine_stack = idx == 0

            material_id = machine_info["parser"]["containers"][str(_ContainerIndexes.Material)]

            is_read_only = self._container_registry.isReadOnly(material_id)
            is_material_in_registry = False
            if not is_read_only:
                material_list = self._container_registry.findInstanceContainersMetadata(id = material_id, type = "material")
                is_material_in_registry = len(material_list) > 0

            if is_machine_stack:
                material_result["machine"] = {"id": material_id,
                                              "is_read_only": is_read_only,
                                              "is_material_in_registry": is_material_in_registry}
            else:
                position = stack_info["parser"]["metadata"]["position"]
                material_result["extruders"][position] = {"id": material_id,
                                                          "is_read_only": is_read_only,
                                                          "is_material_in_registry": is_material_in_registry}
        return material_result

    def __fetchVariantsInfo(self, container_info_dict, all_info_dict, machine_definition) -> dict:
        machine_info = list(container_info_dict["machine"].values())[0]
        stack_info_list = [machine_info] + list(container_info_dict["extruder_train"].values())

        variant_result = {"machine": dict(),
                          "extruders": defaultdict(dict)}
        for idx, stack_info in enumerate(stack_info_list):
            is_machine_stack = idx == 0

            result = variant_result["machine"]
            if not is_machine_stack:
                position = stack_info["parser"]["metadata"]["position"]
                result = variant_result["extruders"][position]

            variant_id = machine_info["parser"]["containers"][str(_ContainerIndexes.Variant)]
            result["id"] = variant_id

        return variant_result

    def __fetchDefinitionChangesValues(self, container_info_dict, all_info_dict, machine_definition) -> dict:
        machine_info = list(container_info_dict["machine"].values())[0]

        def_changes_id = machine_info["parser"]["containers"][str(_ContainerIndexes.DefinitionChanges)]
        def_changes_info = all_info_dict[def_changes_id]

        def_changes_result = {"machine": dict(),
                              "extruders": defaultdict(dict)}
        # TODO: Fix this
        for key, value in def_changes_info["parser"]["values"].items():
            if machine_definition.getProperty(key, "settable_per_extruder"):
                def_changes_info["extruders"]["0"][key] = value
            else:
                def_changes_info["machine"][key] = value
        for extruder_info in container_info_dict["extruder_train"].values():
            position = extruder_info["parser"]["metadata"]["position"]
            quality_changes_id = extruder_info["parser"]["containers"][str(_ContainerIndexes.DefinitionChanges)]
            quality_changes_info = all_info_dict[quality_changes_id]

            for key, value in quality_changes_info["parser"]["values"].items():
                def_changes_info["extruders"][position][key] = value

        return def_changes_result

    def _getMachineInfo(self, archive, container_info_dict, all_info_dict, summary_dict, validation_result_dict):
        container_registry = ContainerRegistry.getInstance()
        need_add_single_extruder = validation_result_dict["need_add_single_extruder"]

        empty_quality_metadata = container_registry.findInstanceContainersMetadata(id = "empty_quality")[0]

        machine_info = list(container_info_dict["machine"].values())[0]

        machine_name = machine_info["parser"]["general"]["name"]
        machine_definition_id = machine_info["parser"]["containers"]["6"]
        machine_definition_list = container_registry.findDefinitionContainers(id = machine_definition_id)
        if not machine_definition_list:
            raise RuntimeError("Cannot get definition with ID %s" % machine_definition_id)
        machine_definition = machine_definition_list[0]
        machine_type_name = machine_definition.getName()

        machine_metadata_list = container_registry.findContainerStacksMetadata(name = machine_name)
        machine_name_exists = len(machine_metadata_list) > 0
        existing_machine_id = None
        if machine_name_exists:
            existing_machine_id = machine_metadata_list[0]["id"]

        user_results = self.__fetchUserChangesInfo(container_info_dict, all_info_dict, machine_definition)
        quality_results = self.__fetchQualityInfo(container_info_dict, all_info_dict, machine_definition)
        user_values_count = user_results.get("total_settings", 0)
        quality_changes_values_count = quality_results.get("total_settings", 0)
        quality_name = quality_results.get("name", empty_quality_metadata["name"])
        quality_type = quality_results.get("quality_type", empty_quality_metadata["quality_type"])

        material_labels = []
        for file_name in container_info_dict["material"]:
            material_labels.append(self._getMaterialLabelFromSerialized(archive.open(file_name).read().decode("utf-8")))

        # TODO
        material_results = self.__fetchMaterialsInfo(container_info_dict, all_info_dict, machine_definition)
        variant_results = self.__fetchVariantsInfo(container_info_dict, all_info_dict, machine_definition)
        def_changes_results = self.__fetchDefinitionChangesValues(container_info_dict, all_info_dict, machine_definition)

        machine_data_dict = {"machine": {"name": machine_name,
                                         "definition_id": machine_definition_id,
                                         "name_exists": machine_name_exists,
                                         "existing_machine_id": existing_machine_id},
                             "user": user_results,
                             "quality": quality_results,
                             "material": material_results,
                             "variant": variant_results,
                             "definition_changes": def_changes_results}

        # check for conflicts
        conflicts_dict = self.__checkForConflicts(machine_data_dict)

        summary_info = {"machine_name": machine_name,
                        "machine_type": machine_type_name,
                        "quality_name": quality_name,
                        "quality_type": quality_type,
                        "quality_changes_settings_count": quality_changes_values_count,
                        "user_settings_count": user_values_count,
                        "material_labels": material_labels,
                        }
        summary_dict.update(summary_info)
        summary_dict.update(conflicts_dict)
        return summary_dict

    def __checkForConflicts(self, machine_data_dict: dict) -> dict:
        has_machine_conflicts = False
        has_quality_conflicts = False
        has_material_conflicts = False
        must_create_new = False

        if machine_data_dict["machine"]["name_exists"]:
            machine_id = machine_data_dict["machine"]["existing_machine_id"]
            machine_stack = self._container_registry.findContainerStacks(id = machine_id)[0]
            machine_definition_id = machine_stack.definition.getId()
            if machine_data_dict["machine"]["definition_id"] != machine_definition_id:
                # If the loaded machine and the existing machine are of different type (such as "um2" and "um3"),
                # always create new.
                must_create_new = True
            else:
                # Get extruder stacks
                extruder_stacks = self._container_registry.findContainerStacks(machine = machine_id,
                                                                               type = "extruder_train")
                extruder_dict = {}
                for stack in extruder_stacks:
                    position = stack.getMetaDataEntry("position")
                    extruder_dict[position] = stack
                # TODO: Fix for single extrusion?

                has_machine_conflicts = self.__checkForMachineConflicts(machine_data_dict,
                                                                        machine_stack, extruder_stacks)

        return {"has_machine_conflicts": has_machine_conflicts,
                "has_quality_conflicts": has_quality_conflicts,
                "has_material_conflicts": has_material_conflicts,
                "must_create_new": must_create_new}

    def __checkForMachineConflicts(self, machine_data_dict, machine_stack, extruder_stacks):
        has_machine_conflicts = False
        for idx, stack in enumerate([machine_stack] + extruder_stacks):
            is_machine_stack = idx == 0

            for container_idx, node_key in ((_ContainerIndexes.UserChanges, "user"),
                                            (_ContainerIndexes.DefinitionChanges, "definition_changes")):
                container = stack.getContainer(container_idx)
                data_dict = machine_data_dict[node_key]

                value_dict = {}
                for key in container.getAllKeys():
                    value_dict[key] = str(container.getProperty(key, "value"))

                if is_machine_stack:
                    has_machine_conflicts = data_dict["machine"] != value_dict
                else:
                    position = stack.getMetaDataEntry("position")
                    has_machine_conflicts = data_dict["extruders"].get(position, {}) != value_dict

                if has_machine_conflicts:
                    return has_machine_conflicts

            # check materials and variants
            for container_idx, node_key in ((_ContainerIndexes.Variant, "variant"),
                                            (_ContainerIndexes.Material, "material")):
                container_id = stack.getContainer(container_idx).getId()
                data_dict = machine_data_dict[node_key]

                if is_machine_stack:
                    has_machine_conflicts = data_dict["machine"]["id"] != container_id
                else:
                    position = stack.getMetaDataEntry("position")
                    has_machine_conflicts = data_dict["extruders"][position]["id"] != container_id
                if has_machine_conflicts:
                    return has_machine_conflicts

        quality_changes_id = machine_stack.qualityChanges.getId()
        if quality_changes_id != "empty_quality_changes":
            quality_dict = {"name": machine_stack.qualityChanges.getName(),
                            "quality_type": machine_stack.qualityChanges.getMetaDataEntry("quality_type"),
                            "is_custom_quality": True}
        else:
            quality_dict = {"name": machine_stack.quality.getName(),
                            "quality_type": machine_stack.quality.getMetaDataEntry("quality_type"),
                            "is_custom_quality": False}

        _machine_quality_dict = {k: machine_data_dict["quality"][k] for k in quality_dict}
        if quality_dict != _machine_quality_dict:
            has_machine_conflicts = True

        return has_machine_conflicts

    @call_on_qt_thread
    def preRead(self, file_name, show_dialog = True, *args, **kwargs):
        self._resetState()

        if self._3mf_mesh_reader is None:
            self._3mf_mesh_reader = Application.getInstance().getMeshFileHandler().getReaderForFile(file_name)
            if not (self._3mf_mesh_reader and self._3mf_mesh_reader.preRead(file_name) == WorkspaceReader.PreReadResult.accepted):
                Logger.log("e", "Could not find reader that was able to read the scene data for 3MF workspace")
                return WorkspaceReader.PreReadResult.failed

        # Check if there are any conflicts, so we can ask the user.
        archive = zipfile.ZipFile(file_name, "r")
        try:
            info_dict_by_type, all_info_dict, validation_result_dict = self._loadFilesAndValidate(archive)
        except:
            Logger.logException("e", "Invalid project file %s", file_name)
            return WorkspaceReader.PreReadResult.failed

        summary_dict = dict()
        self._loadSummaryInfoFromPreferences(archive, summary_dict)
        machine_info_dict = self._getMachineInfo(archive, info_dict_by_type, all_info_dict, summary_dict,
                                                 validation_result_dict)

        # Cache results so later in read(), we don't need to do that again
        self._info_dict_by_type = info_dict_by_type
        self._all_info_dict = all_info_dict
        self._validation_result_dict = validation_result_dict
        self._machine_info_dict = machine_info_dict
        self._summary_dict = summary_dict

        # Show the dialog, informing the user what is about to happen.
        self._dialog.setMachineName(summary_dict["machine_name"])
        self._dialog.setMachineType(summary_dict["machine_type"])
        self._dialog.setMaterialLabels(summary_dict["material_labels"])
        self._dialog.setQualityName(summary_dict["quality_name"])
        self._dialog.setQualityType(summary_dict["quality_type"])
        self._dialog.setNumSettingsOverridenByQualityChanges(summary_dict["quality_changes_settings_count"])
        self._dialog.setNumUserSettings(summary_dict["user_settings_count"])

        self._dialog.setActiveMode(summary_dict["active_mode"])
        self._dialog.setHasVisibleSettingsField(summary_dict["has_visible_settings"])
        self._dialog.setNumVisibleSettings(summary_dict["visible_settings_count"])

        self._dialog.setMachineConflict(summary_dict["has_machine_conflicts"])
        self._dialog.setQualityChangesConflict(summary_dict["has_quality_conflicts"])
        self._dialog.setMaterialConflict(summary_dict["has_material_conflicts"])

        self._dialog.setVariantType(i18n_catalog.i18nc("@label", "Nozzle"))
        self._dialog.setHasObjectsOnPlate(Application.getInstance().platformActivity)
        self._dialog.show()

        # Block until the dialog is closed.
        self._dialog.waitForClose()

        if self._dialog.getResult() == {}:
            return WorkspaceReader.PreReadResult.cancelled

        self._resolve_strategies = self._dialog.getResult()
        #
        # There can be 3 resolve strategies coming from the dialog:
        #  - new:       create a new container
        #  - override:  override the existing container
        #  - None:      There is no conflict, which means containers with the same IDs may or may not be there already.
        #               If there is an existing container, there is no conflict between them, and default to "override"
        #               If there is no existing container, default to "new"
        #
        resolve_strategies = dict()
        if not summary_dict["has_machine_conflicts"]:
            machine_resolve_strategy = "new"
        else:
            machine_resolve_strategy = self._resolve_strategies.get("machine", "override")
            machine_resolve_strategy = "override" if not machine_resolve_strategy else machine_resolve_strategy
            if summary_dict["must_create_new"]:
                machine_resolve_strategy = "new"
        resolve_strategies["machine"] = machine_resolve_strategy

        # If we must create a new machine, also create new for materials and qualities
        if machine_resolve_strategy == "new":
            resolve_strategies["material"] = "new"
            resolve_strategies["quality"] = "new"
        else:
            for key in ("material", "quality"):
                conflict_key = "has_%s_conflicts" % key
                if not summary_dict[conflict_key]:
                    resolve_strategies[key] = "new"
                else:
                    resolve_strategies[key] = self._resolve_strategies.get(key, "override")
                    resolve_strategies[key] = "override" if not resolve_strategies[key] else resolve_strategies[key]

        self._resolve_strategies = resolve_strategies

        return WorkspaceReader.PreReadResult.accepted

    @call_on_qt_thread
    def read(self, file_name):
        # TODO: get a machine, either reuse an existing one or create a new one
        reuse_existing_machine = self._summary_dict["has_machine_conflicts"] and not self._summary_dict["must_create_new"]
        if reuse_existing_machine:
            # TODO:
            pass

        # Load all the nodes / meshdata of the workspace
        nodes = self._3mf_mesh_reader.read(file_name)
        if nodes is None:
            nodes = []

        base_file_name = os.path.basename(file_name)
        if base_file_name.endswith(".curaproject.3mf"):
            base_file_name = base_file_name[:base_file_name.rfind(".curaproject.3mf")]
        self.setWorkspaceName(base_file_name)
        return nodes

    def _getXmlProfileClass(self):
        return self._container_registry.getContainerForMimeType(MimeTypeDatabase.getMimeType("application/x-ultimaker-material-profile"))

    def _getMaterialLabelFromSerialized(self, serialized):
        data = ET.fromstring(serialized)
        metadata = data.iterfind("./um:metadata/um:name/um:label", {"um": "http://www.ultimaker.com/material"})
        for entry in metadata:
            return entry.text
