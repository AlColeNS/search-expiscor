/*
 * NorthRidge Software, LLC - Copyright (c) 2019.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.nridge.core.base.field.data;

import com.nridge.core.base.field.*;
import com.nridge.core.base.std.NSException;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.*;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;

/**
 * The DataBeanBag is responsible for mapping POJOs to
 * DataBag instances.
 * <p>
 * The following article by Jakob Jenkov was helpful in deciphering
 * private field and method access.
 * http://tutorials.jenkov.com/java-reflection/private-fields-and-methods.html
 * </p>
 * @author Al Cole
 * @version 1.0 Jan 2, 2014
 * @since 1.0
 */
@SuppressWarnings("unchecked")
public class DataBeanBag
{
    private DataBeanBag()
    {
    }

    private static DataField reflectField(Object anObject, Field aField)
        throws NoSuchFieldException, IllegalAccessException
    {
        DataField dataField;
        String fieldName, fieldValue, fieldTitle;
        com.nridge.core.base.field.Field.Type fieldType;

        fieldName = aField.getName();
        fieldTitle = com.nridge.core.base.field.Field.nameToTitle(fieldName);

        Class<?> classFieldType = aField.getType();
        Object fieldObject = aField.get(anObject);

        if ((StringUtils.endsWith(classFieldType.getName(), "Integer")) ||
            (StringUtils.endsWith(classFieldType.getName(), "int")))
        {
            fieldType = com.nridge.core.base.field.Field.Type.Integer;
            fieldValue = fieldObject.toString();
            dataField = new DataField(fieldType, fieldName, fieldTitle, fieldValue);
        }
        else if ((StringUtils.endsWith(classFieldType.getName(), "Float")) ||
                 (StringUtils.endsWith(classFieldType.getName(), "float")))
        {
            fieldType = com.nridge.core.base.field.Field.Type.Float;
            fieldValue = fieldObject.toString();
            dataField = new DataField(fieldType, fieldName, fieldTitle, fieldValue);
        }
        else if ((StringUtils.endsWith(classFieldType.getName(), "Double")) ||
                 (StringUtils.endsWith(classFieldType.getName(), "double")))
        {
            fieldType = com.nridge.core.base.field.Field.Type.Double;
            fieldValue = fieldObject.toString();
            dataField = new DataField(fieldType, fieldName, fieldTitle, fieldValue);
        }
        else if ((StringUtils.endsWith(classFieldType.getName(), "Boolean")) ||
                (StringUtils.endsWith(classFieldType.getName(), "boolean")))
        {
            fieldType = com.nridge.core.base.field.Field.Type.Boolean;
            fieldValue = fieldObject.toString();
            dataField = new DataField(fieldType, fieldName, fieldTitle, fieldValue);
        }
        else if (StringUtils.endsWith(classFieldType.getName(), "Date"))
        {
            fieldType = com.nridge.core.base.field.Field.Type.Date;
            Date fieldDate = (Date) fieldObject;
            fieldValue = com.nridge.core.base.field.Field.dateValueFormatted(fieldDate, null);
            dataField = new DataField(fieldType, fieldName, fieldTitle, fieldValue);
        }
        else
        {
            fieldType = com.nridge.core.base.field.Field.Type.Text;
            if (fieldObject instanceof String[])
            {
                String[] fieldValues = (String[]) fieldObject;
                dataField = new DataField(fieldType, fieldName, fieldTitle);
                dataField.setMultiValueFlag(true);
                dataField.setValues(fieldValues);
            }
            else if (fieldObject instanceof Collection)
            {
                ArrayList<String> fieldValues = (ArrayList<String>) fieldObject;
                dataField = new DataField(fieldType, fieldName, fieldTitle);
                dataField.setMultiValueFlag(true);
                dataField.setValues(fieldValues);
            }
            else
            {
                fieldValue = fieldObject.toString();
                dataField = new DataField(fieldType, fieldName, fieldTitle, fieldValue);
            }
        }

        return dataField;
    }

    private static DataField reflectMethod(Object anObject, BeanField aBeanField, Method aMethod)
        throws NSException, IllegalAccessException, InvocationTargetException
    {
        String fieldValue;
        DataField dataField;
        com.nridge.core.base.field.Field.Type fieldType;

        if (anObject == null)
            throw new NSException("Object is null");
        String fieldName = aBeanField.name();
        if (StringUtils.isEmpty(fieldName))
            throw new NSException("Field name not identified in annotation.");

        String fieldTitle = aBeanField.title();
        if (StringUtils.isEmpty(fieldTitle))
            fieldTitle = com.nridge.core.base.field.Field.nameToTitle(fieldName);
        String fieldTypeString = aBeanField.type();
        if (StringUtils.isEmpty(fieldTypeString))
        {
            Class<?> methodReturnType = aMethod.getReturnType();
            if ((StringUtils.endsWith(methodReturnType.getName(), "Integer")) ||
                (StringUtils.endsWith(methodReturnType.getName(), "int")))
            {
                fieldType = com.nridge.core.base.field.Field.Type.Integer;
                fieldValue = aMethod.invoke(anObject).toString();
                dataField = new DataField(fieldType, fieldName, fieldTitle, fieldValue);
            }
            else if ((StringUtils.endsWith(methodReturnType.getName(), "Float")) ||
                     (StringUtils.endsWith(methodReturnType.getName(), "float")))
            {
                fieldType = com.nridge.core.base.field.Field.Type.Float;
                fieldValue = aMethod.invoke(anObject).toString();
                dataField = new DataField(fieldType, fieldName, fieldTitle, fieldValue);
            }
            else if ((StringUtils.endsWith(methodReturnType.getName(), "Double")) ||
                     (StringUtils.endsWith(methodReturnType.getName(), "double")))
            {
                fieldType = com.nridge.core.base.field.Field.Type.Double;
                fieldValue = aMethod.invoke(anObject).toString();
                dataField = new DataField(fieldType, fieldName, fieldTitle, fieldValue);
            }
            else if ((StringUtils.endsWith(methodReturnType.getName(), "Boolean")) ||
                     (StringUtils.endsWith(methodReturnType.getName(), "boolean")))
            {
                fieldType = com.nridge.core.base.field.Field.Type.Boolean;
                fieldValue = aMethod.invoke(anObject).toString();
                dataField = new DataField(fieldType, fieldName, fieldTitle, fieldValue);
            }
            else if (StringUtils.endsWith(methodReturnType.getName(), "Date"))
            {
                fieldType = com.nridge.core.base.field.Field.Type.Date;
                Date fieldDate = (Date) aMethod.invoke(anObject);
                fieldValue = com.nridge.core.base.field.Field.dateValueFormatted(fieldDate, null);
                dataField = new DataField(fieldType, fieldName, fieldTitle, fieldValue);
            }
            else
            {
                fieldType = com.nridge.core.base.field.Field.Type.Text;
                Object methodReturnObject = aMethod.invoke(anObject);
                if (methodReturnObject instanceof String[])
                {
                    String[] fieldValues = (String[]) methodReturnObject;
                    dataField = new DataField(fieldType, fieldName, fieldTitle);
                    dataField.setMultiValueFlag(true);
                    dataField.setValues(fieldValues);
                }
                else if (methodReturnObject instanceof Collection)
                {
                    ArrayList<String> fieldValues = (ArrayList<String>) methodReturnObject;
                    dataField = new DataField(fieldType, fieldName, fieldTitle);
                    dataField.setMultiValueFlag(true);
                    dataField.setValues(fieldValues);
                }
                else
                {
                    fieldValue = aMethod.invoke(anObject).toString();
                    dataField = new DataField(fieldType, fieldName, fieldTitle, fieldValue);
                }
            }
        }
        else
        {
            fieldType = com.nridge.core.base.field.Field.stringToType(fieldTypeString);
            Object methodReturnObject = aMethod.invoke(anObject);
            if (methodReturnObject instanceof String[])
            {
                String[] fieldValues = (String[]) methodReturnObject;
                dataField = new DataField(fieldType, fieldName, fieldTitle);
                dataField.setMultiValueFlag(true);
                dataField.setValues(fieldValues);
            }
            else if (methodReturnObject instanceof Collection)
            {
                ArrayList<String> fieldValues = (ArrayList<String>) methodReturnObject;
                dataField = new DataField(fieldType, fieldName, fieldTitle);
                dataField.setMultiValueFlag(true);
                dataField.setValues(fieldValues);
            }
            else
            {
                fieldValue = methodReturnObject.toString();
                dataField = new DataField(fieldType, fieldName, fieldTitle, fieldValue);
            }
        }

        return dataField;
    }

    /**
     * Accepts a POJO containing one or more public fields and
     * creates a DataBag from them.  The DataBeanObject1 test
     * class provides a reference example.
     *
     * @param anObject POJO instance.
     *
     * @return Data bag instance populated with field information.
     *
     * @throws NSException Thrown if object is null.
     * @throws NoSuchFieldException Thrown if field does not exist.
     * @throws IllegalAccessException Thrown if access is illegal.
     */
    public static DataBag fromFieldsToBag(Object anObject)
        throws NSException, NoSuchFieldException, IllegalAccessException
    {
        DataField dataField;
        boolean isPublicAccess;

        if (anObject == null)
            throw new NSException("Object is null");

        DataBag dataBag = new DataBag(anObject.toString());

        Class<?> objClass = anObject.getClass();
        Field[] fieldArray = objClass.getDeclaredFields();
        for (Field objField : fieldArray)
        {
            isPublicAccess = Modifier.isPublic(objField.getModifiers());
            if (! isPublicAccess)
                objField.setAccessible(true);
            dataField = reflectField(anObject, objField);
            dataBag.add(dataField);

        }

        return dataBag;
    }

    /**
     * Accepts a POJO containing one or more public annotated get
     * methods and creates a DataBag from them.  The DataBeanObject2
     * test class provides a reference example.
     *
     * @param anObject POJO instance.
     *
     * @return Data bag instance populated with field information.
     *
     * @throws NSException Thrown if object is null.
     * @throws IllegalAccessException Thrown if access is illegal.
     * @throws InvocationTargetException Thrown if target execution fails.
     */
    public static DataBag fromMethodsToBag(Object anObject)
        throws NSException, IllegalAccessException, InvocationTargetException
    {
        DataField dataField;
        BeanField beanField;
        boolean isPublicAccess, isAnnotationPresent;

        if (anObject == null)
            throw new NSException("Object is null");

        DataBag dataBag = new DataBag(anObject.toString());
        Class<?> objClass = anObject.getClass();
        Method[] methodArray = objClass.getDeclaredMethods();
        for (Method objMethod : methodArray)
        {
            isPublicAccess = Modifier.isPublic(objMethod.getModifiers());
            isAnnotationPresent = objMethod.isAnnotationPresent(BeanField.class);
            if ((isAnnotationPresent) && (isPublicAccess))
            {
                beanField = objMethod.getAnnotation(BeanField.class);
                dataField = reflectMethod(anObject, beanField, objMethod);
                dataBag.add(dataField);
            }
        }

        return dataBag;
    }
}
