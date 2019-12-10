#!/usr/bin/env/python
#! -*- encoding: utf-8 -*-

class XmlDict(dict):
    """
    A class for a XML data. XML structure is similar to python dictionary structure but allows to use structure with
    more that one the same tag like:
    <tag1>
        <tag2>content1</tag2>
        <tag2>content2</tag2>
        <tag2>content3</tag2>
    </tag1>
    Therefore in this class added method add_tag which add to the dictionary content of a tag:
    {tag1:
        {tag2:[content1,
               content2,
               content3]
        }
    }
    """
    def add_tag(self, tag: str, content):
        """
        Adds content under the tag in the inner dictionary. If the dictionary already contains something
        under this tag - add content to a list under this tag.
        :param tag: A xml tag
        :param content: Content under the tag. It can be string or dictionary
        :return: None
        """
        if tag in self:
            if type(self[tag]) == type(content):
                self[tag] = [self[tag], content]
            elif type(self[tag]) is list:
                self[tag].append(content)
            else:
                print("Wrong tag <{}> content".format(tag))
                exit(-1)
        else:
            self[tag] = content

    @staticmethod
    def parse_xml(path_to_xml, encoding="utf8"):
        """
        Parses a xml file and returns it's content as dictionary
        :param path_to_xml: path to an xml file
        :return: dictionary with tags as keys
        """
        def read_xml(file_content: str):
            xml_dict = XmlDict()
            i = 0
            while i < len(file_content):
                if file_content[i] == '<':
                    tag = find_tag(file_content[i:])
                    i += len(tag)
                    if tag[-2] == '/':  # If an opening tag contains '/' - it's a complex tag or an empty tag
                        if ' ' in tag:
                            tag_content = parse_tag(tag)  # Obtain the tag's content
                            tag = tag[1:tag.find(' ')]  # Get the tag name
                        else:  # If the tag contains '/' but doesn't contain ' ' it's an empty tag - <tag/>
                            tag_content = None
                            tag = tag[1:-2]
                    else:  # Usual opening tag - <tag>
                        tag = tag[1:-1]
                        closing_tag = "</{}>".format(tag)
                        k = i
                        while file_content[i:i + len(closing_tag)] != closing_tag:
                            i += 1
                        tag_content = read_xml(file_content[k:i])  # Search for the tag's content
                        i += len(closing_tag)
                    xml_dict.add_tag(tag, tag_content)
                else:
                    return file_content
            return xml_dict

        def find_tag(file_string: str) -> str:
            """
            Searches through the file for a tag
            :param file_string: The entire file/left file as one string
            :return: tag
            """
            data_len = len(file_string)
            for i in range(len(file_string)):
                if file_string[i] == '<':
                    k = i
                    while file_string[k] != '>' and k < data_len:
                        k += 1
                    return file_string[i:k + 1]

        def parse_tag(complex_tag: str) -> dict:
            """
            Parses a complex tag '<tag field1="s1" field2="s2"/>' to 'field1="s1" field2="s2"'
            :param complex_tag:  a complex tag with data structure
            :return: dictionary with keys as field names and descriptions as values
            """
            content = complex_tag[
                      complex_tag.find(' '):-3]  # '<tag field1="s1" field2="s2"/>' to 'field1="s1" field2="s2"'
            content = content.split('"')
            return {content[2 * i][1:-1]: content[2 * i + 1] for i in range(int(len(content) / 2))}

        def to_line(file_content: list) -> str:
            """
            Converts list of lines to one line without \t, \n , ' ' between tags
            :param file_content: list of lines of a xml file
            :return: string of the xml file
            """
            lines = []
            for i in range(len(file_content)):
                if file_content[i] == '\n':
                    continue
                if file_content[i][-1] == '\n':
                    lines.append(file_content[i][file_content[i].find('<'):-1])
                else:
                    lines.append(file_content[-1][file_content[i].find('<'):])
            return ''.join(lines)

        with open(path_to_xml, encoding=encoding) as file:
            header = file.readline()[:-1]
            if header[:2] == "<?" and header[-2:] == "?>":
                file_content = to_line(file.readlines())
                return read_xml(file_content)
            else:
                print("Wrong header: {}".format(header))
            exit(1)

    @staticmethod
    def save_xml(dictionary, path_to_save, encoding="utf8"):
        """
        Saves a dictionary to an xml file
        :param dictionary: A dictionary which should be saved
        :param path_to_save: A path where the .xml file should be saved
        :return:
        """
        def text_xml(dictionary, level=0):
            text = ""
            tags = list(dictionary.keys())
            keys = []
            for i in range(len(tags)):  # Run through the dictionary
                if type(dictionary[tags[i]]) is str or dictionary[tags[i]] is None:
                    keys.append(tags[i])  # Add <tag>value</tag> or <tag/> tags first
            for i in range(len(tags)):
                if not (type(dictionary[tags[i]]) is str or dictionary[tags[i]] is None):
                    keys.append(tags[i])  # Add complex tags (dictionary, list) at the end

            for key in keys:
                if dictionary[key] is None or dictionary[key] == '':
                    text += level * '\t'
                    text += "<{}/>\n".format(key)
                elif type(dictionary[key]) is str:
                    text += level * '\t'
                    text += "<{}>{}</{}>\n".format(key, dictionary[key], key)
                elif type(dictionary[key]) is XmlDict or type(dictionary[key]) is dict:
                    text += level * '\t'
                    text += "<{}>\n{}{}</{}>\n".format(key, text_xml(dictionary[key], level + 1), level * '\t', key)
                elif type(dictionary[key]) is list:
                    content = ["{}<{}>\n{}{}</{}>\n".format(level * '\t', key,
                                                            text_xml(dictionary[key][i], level + 1),
                                                            level * '\t', key) for i in range(len(dictionary[key]))]
                    text += ''.join(content)
            return text

        with open(path_to_save, encoding=encoding, mode='w') as file:
            text = text_xml(dictionary)
            file.write('<?xml version="1.0" encoding="UTF-8"?>\n')
            file.write(text)
            file.close()


if __name__ == "__main__":
    print("XMLDict class v0.1. Parses xml 1.0 and xml 1.1 to dictionary tree. Saves to xml 1.0")
